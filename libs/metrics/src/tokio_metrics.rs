use std::collections::HashMap;

use once_cell::sync::Lazy;
use prometheus::{
    core::Desc,
    proto::{self, LabelPair},
};

#[derive(Default)]
pub struct TokioCollector {
    handles: Vec<tokio::runtime::Handle>,
}

impl TokioCollector {
    pub fn add_runtime(&mut self, handle: tokio::runtime::Handle) {
        self.handles.push(handle);
    }

    fn guage(
        &self,
        desc: &Desc,
        f: impl Fn(&tokio::runtime::RuntimeMetrics) -> f64,
    ) -> proto::MetricFamily {
        self.metric(desc, proto::MetricType::GAUGE, |rt| {
            let mut g = proto::Gauge::default();
            g.set_value(f(rt));
            let mut m = proto::Metric::default();
            m.set_gauge(g);
            m
        })
    }

    fn metric(
        &self,
        desc: &Desc,
        typ: proto::MetricType,
        f: impl Fn(&tokio::runtime::RuntimeMetrics) -> proto::Metric,
    ) -> proto::MetricFamily {
        let mut m = proto::MetricFamily::default();
        m.set_name(desc.fq_name.clone());
        m.set_help(desc.help.clone());
        m.set_field_type(typ);
        let mut metrics = vec![];
        for rt in &self.handles {
            let rt_metrics = rt.metrics();
            let mut m = f(&rt_metrics);

            let mut label_id = LabelPair::default();
            label_id.set_name("id".to_owned());
            label_id.set_value(rt.id().to_string());

            m.set_label(vec![label_id]);
            metrics.push(m);
        }

        m.set_metric(metrics);
        m
    }

    // fn guage_per_worker(
    //     &self,
    //     desc: &Desc,
    //     f: impl Fn(&tokio::runtime::RuntimeMetrics, usize) -> f64,
    // ) -> proto::MetricFamily {
    //     self.metric_per_worker(desc, proto::MetricType::GAUGE, |m, i| {
    //         let mut g = proto::Gauge::default();
    //         g.set_value(f(m, i));
    //         let mut m = proto::Metric::default();
    //         m.set_gauge(g);
    //         m
    //     })
    // }

    fn counter_per_worker(
        &self,
        desc: &Desc,
        f: impl Fn(&tokio::runtime::RuntimeMetrics, usize) -> f64,
    ) -> proto::MetricFamily {
        self.metric_per_worker(desc, proto::MetricType::COUNTER, |m, i| {
            let mut g = proto::Counter::default();
            g.set_value(f(m, i));
            let mut m = proto::Metric::default();
            m.set_counter(g);
            m
        })
    }

    fn metric_per_worker(
        &self,
        desc: &Desc,
        typ: proto::MetricType,
        f: impl Fn(&tokio::runtime::RuntimeMetrics, usize) -> proto::Metric,
    ) -> proto::MetricFamily {
        let mut m = proto::MetricFamily::default();
        m.set_name(desc.fq_name.clone());
        m.set_help(desc.help.clone());
        m.set_field_type(typ);
        let mut metrics = vec![];
        for rt in &self.handles {
            let rt_metrics = rt.metrics();
            let workers = rt_metrics.num_workers();

            for i in 0..workers {
                let mut m = f(&rt_metrics, i);

                let mut label_id = LabelPair::default();
                label_id.set_name("id".to_owned());
                label_id.set_value(rt.id().to_string());

                let mut label_worker = LabelPair::default();
                label_worker.set_name("worker".to_owned());
                label_worker.set_value(i.to_string());

                m.set_label(vec![label_id, label_worker]);
                metrics.push(m);
            }
        }

        m.set_metric(metrics);
        m
    }
}

static ACTIVE_TASK_COUNT: Lazy<Desc> = Lazy::new(|| {
    Desc::new(
        "tokio_active_task_count_total".to_owned(),
        "the number of active tasks in the runtime".to_owned(),
        vec!["id".to_owned()],
        HashMap::default(),
    )
    .expect("should be a valid description")
});
static WORKER_STEAL_COUNT: Lazy<Desc> = Lazy::new(|| {
    Desc::new(
        "tokio_worker_steal_count_total".to_owned(),
        "the number of tasks the given worker thread stole from another worker thread".to_owned(),
        vec!["id".to_owned(), "worker".to_owned()],
        HashMap::default(),
    )
    .expect("should be a valid description")
});

impl prometheus::core::Collector for TokioCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&ACTIVE_TASK_COUNT, &WORKER_STEAL_COUNT]
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        vec![
            self.guage(&ACTIVE_TASK_COUNT, |m| m.active_tasks_count() as f64),
            self.counter_per_worker(&WORKER_STEAL_COUNT, |m, i| m.worker_steal_count(i) as f64),
        ]
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use prometheus::{Registry, TextEncoder};
    use tokio::{runtime, sync::Mutex, task::JoinSet};

    use crate::tokio_metrics::TokioCollector;

    // this will get flaky if we have other tokio tests in this crate...
    #[test]
    fn gather_multiple_runtimes() {
        let registry = Registry::new();
        let mut collector = TokioCollector::default();

        let runtime1 = runtime::Builder::new_current_thread().build().unwrap();
        let runtime2 = runtime::Builder::new_current_thread().build().unwrap();

        collector.add_runtime(runtime1.handle().clone());
        collector.add_runtime(runtime2.handle().clone());

        registry.register(Box::new(collector)).unwrap();

        std::thread::scope(|s| {
            let lock1 = Arc::new(Mutex::new(0));
            let lock2 = Arc::new(Mutex::new(0));

            let guard1 = lock1.clone().try_lock_owned().unwrap();
            let guard2 = lock2.clone().try_lock_owned().unwrap();

            s.spawn(move || {
                runtime1.block_on(async {
                    let mut joinset = JoinSet::new();
                    for _ in 0..5 {
                        let lock = lock1.clone();
                        joinset.spawn(async move {
                            let mut _guard = lock.lock().await;
                        });
                    }
                    while let Some(_x) = joinset.join_next().await {}
                })
            });

            s.spawn(move || {
                runtime2.block_on(async {
                    let mut joinset = JoinSet::new();
                    for _ in 0..5 {
                        let lock = lock2.clone();
                        joinset.spawn(async move {
                            let mut _guard = lock.lock().await;
                        });
                    }
                    while let Some(_x) = joinset.join_next().await {}
                })
            });

            std::thread::sleep(Duration::from_millis(10));

            let text = TextEncoder.encode_to_string(&registry.gather()).unwrap();
            assert_eq!(
                text,
                r#"# HELP tokio_active_task_count_total the number of active tasks in the runtime
# TYPE tokio_active_task_count_total gauge
tokio_active_task_count_total{id="1"} 5
tokio_active_task_count_total{id="2"} 5
# HELP tokio_worker_steal_count_total the number of tasks the given worker thread stole from another worker thread
# TYPE tokio_worker_steal_count_total counter
tokio_worker_steal_count_total{id="1",worker="0"} 0
tokio_worker_steal_count_total{id="2",worker="0"} 0
"#
            );

            drop(guard1);
            std::thread::sleep(Duration::from_millis(10));

            let text = TextEncoder.encode_to_string(&registry.gather()).unwrap();
            assert_eq!(
                text,
                r#"# HELP tokio_active_task_count_total the number of active tasks in the runtime
# TYPE tokio_active_task_count_total gauge
tokio_active_task_count_total{id="1"} 0
tokio_active_task_count_total{id="2"} 5
# HELP tokio_worker_steal_count_total the number of tasks the given worker thread stole from another worker thread
# TYPE tokio_worker_steal_count_total counter
tokio_worker_steal_count_total{id="1",worker="0"} 0
tokio_worker_steal_count_total{id="2",worker="0"} 0
"#
            );

            drop(guard2);
            std::thread::sleep(Duration::from_millis(10));

            let text = TextEncoder.encode_to_string(&registry.gather()).unwrap();
            assert_eq!(
                text,
                r#"# HELP tokio_active_task_count_total the number of active tasks in the runtime
# TYPE tokio_active_task_count_total gauge
tokio_active_task_count_total{id="1"} 0
tokio_active_task_count_total{id="2"} 0
# HELP tokio_worker_steal_count_total the number of tasks the given worker thread stole from another worker thread
# TYPE tokio_worker_steal_count_total counter
tokio_worker_steal_count_total{id="1",worker="0"} 0
tokio_worker_steal_count_total{id="2",worker="0"} 0
"#
            );
        });
    }
}

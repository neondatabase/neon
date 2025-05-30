use std::collections::HashMap;

use super::*;
use crate::consumption_metrics::RawMetric;

#[test]
fn startup_collected_timeline_metrics_before_advancing() {
    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();

    let mut metrics = Vec::new();
    let cache = HashMap::new();

    let initdb_lsn = Lsn(0x10000);
    let pitr_cutoff = Lsn(0x11000);
    let disk_consistent_lsn = Lsn(initdb_lsn.0 * 2);
    let logical_size = 0x42000;

    let snap = TimelineSnapshot {
        loaded_at: (disk_consistent_lsn, SystemTime::now()),
        last_record_lsn: disk_consistent_lsn,
        ancestor_lsn: Lsn(0),
        current_exact_logical_size: Some(logical_size),
        pitr_enabled: true,
        pitr_cutoff: Some(pitr_cutoff),
    };

    let now = DateTime::<Utc>::from(SystemTime::now());

    snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(
                snap.loaded_at.1.into(),
                now,
                0
            ),
            MetricsKey::written_size(tenant_id, timeline_id).at(now, disk_consistent_lsn.0),
            MetricsKey::written_size_since_parent(tenant_id, timeline_id)
                .at(now, disk_consistent_lsn.0),
            MetricsKey::pitr_history_size_since_parent(tenant_id, timeline_id)
                .at(now, disk_consistent_lsn.0 - pitr_cutoff.0),
            MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, logical_size)
        ]
    );
}

#[test]
fn startup_collected_timeline_metrics_second_round() {
    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();

    let [now, before, init] = time_backwards();

    let now = DateTime::<Utc>::from(now);
    let before = DateTime::<Utc>::from(before);

    let initdb_lsn = Lsn(0x10000);
    let pitr_cutoff = Lsn(0x11000);
    let disk_consistent_lsn = Lsn(initdb_lsn.0 * 2);
    let logical_size = 0x42000;

    let mut metrics = Vec::new();
    let cache = HashMap::from([MetricsKey::written_size(tenant_id, timeline_id)
        .at(before, disk_consistent_lsn.0)
        .to_kv_pair()]);

    let snap = TimelineSnapshot {
        loaded_at: (disk_consistent_lsn, init),
        last_record_lsn: disk_consistent_lsn,
        ancestor_lsn: Lsn(0),
        current_exact_logical_size: Some(logical_size),
        pitr_enabled: true,
        pitr_cutoff: Some(pitr_cutoff),
    };

    snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(before, now, 0),
            MetricsKey::written_size(tenant_id, timeline_id).at(now, disk_consistent_lsn.0),
            MetricsKey::written_size_since_parent(tenant_id, timeline_id)
                .at(now, disk_consistent_lsn.0),
            MetricsKey::pitr_history_size_since_parent(tenant_id, timeline_id)
                .at(now, disk_consistent_lsn.0 - pitr_cutoff.0),
            MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, logical_size)
        ]
    );
}

#[test]
fn startup_collected_timeline_metrics_nth_round_at_same_lsn() {
    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();

    let [now, just_before, before, init] = time_backwards();

    let now = DateTime::<Utc>::from(now);
    let just_before = DateTime::<Utc>::from(just_before);
    let before = DateTime::<Utc>::from(before);

    let initdb_lsn = Lsn(0x10000);
    let pitr_cutoff = Lsn(0x11000);
    let disk_consistent_lsn = Lsn(initdb_lsn.0 * 2);
    let logical_size = 0x42000;

    let mut metrics = Vec::new();
    let cache = HashMap::from([
        // at t=before was the last time the last_record_lsn changed
        MetricsKey::written_size(tenant_id, timeline_id)
            .at(before, disk_consistent_lsn.0)
            .to_kv_pair(),
        // end time of this event is used for the next ones
        MetricsKey::written_size_delta(tenant_id, timeline_id)
            .from_until(before, just_before, 0)
            .to_kv_pair(),
    ]);

    let snap = TimelineSnapshot {
        loaded_at: (disk_consistent_lsn, init),
        last_record_lsn: disk_consistent_lsn,
        ancestor_lsn: Lsn(0),
        current_exact_logical_size: Some(logical_size),
        pitr_enabled: true,
        pitr_cutoff: Some(pitr_cutoff),
    };

    snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(just_before, now, 0),
            MetricsKey::written_size(tenant_id, timeline_id).at(now, disk_consistent_lsn.0),
            MetricsKey::written_size_since_parent(tenant_id, timeline_id)
                .at(now, disk_consistent_lsn.0),
            MetricsKey::pitr_history_size_since_parent(tenant_id, timeline_id)
                .at(now, disk_consistent_lsn.0 - pitr_cutoff.0),
            MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, logical_size)
        ]
    );
}

/// Tests that written sizes do not regress across restarts.
#[test]
fn post_restart_written_sizes_with_rolled_back_last_record_lsn() {
    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();

    let [later, now, at_restart] = time_backwards();

    // FIXME: tests would be so much easier if we did not need to juggle back and forth
    // SystemTime and DateTime::<Utc> ... Could do the conversion only at upload time?
    let now = DateTime::<Utc>::from(now);
    let later = DateTime::<Utc>::from(later);
    let before_restart = at_restart - std::time::Duration::from_secs(5 * 60);
    let way_before = before_restart - std::time::Duration::from_secs(10 * 60);
    let before_restart = DateTime::<Utc>::from(before_restart);
    let way_before = DateTime::<Utc>::from(way_before);

    let snap = TimelineSnapshot {
        loaded_at: (Lsn(50), at_restart),
        last_record_lsn: Lsn(50),
        ancestor_lsn: Lsn(0),
        current_exact_logical_size: None,
        pitr_enabled: true,
        pitr_cutoff: Some(Lsn(20)),
    };

    let mut cache = HashMap::from([
        MetricsKey::written_size(tenant_id, timeline_id)
            .at(before_restart, 100)
            .to_kv_pair(),
        MetricsKey::written_size_delta(tenant_id, timeline_id)
            .from_until(
                way_before,
                before_restart,
                // not taken into account, but the timestamps are important
                999_999_999,
            )
            .to_kv_pair(),
    ]);

    let mut metrics = Vec::new();
    snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(
                before_restart,
                now,
                0
            ),
            MetricsKey::written_size(tenant_id, timeline_id).at(now, 100),
            MetricsKey::written_size_since_parent(tenant_id, timeline_id).at(now, 100),
            MetricsKey::pitr_history_size_since_parent(tenant_id, timeline_id).at(now, 80),
        ]
    );

    // now if we cache these metrics, and re-run while "still in recovery"
    cache.extend(metrics.drain(..).map(|x| x.to_kv_pair()));

    // "still in recovery", because our snapshot did not change
    snap.to_metrics(tenant_id, timeline_id, later, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(now, later, 0),
            MetricsKey::written_size(tenant_id, timeline_id).at(later, 100),
            MetricsKey::written_size_since_parent(tenant_id, timeline_id).at(later, 100),
            MetricsKey::pitr_history_size_since_parent(tenant_id, timeline_id).at(later, 80),
        ]
    );
}

/// Tests that written sizes do not regress across restarts, even on child branches.
#[test]
fn post_restart_written_sizes_with_rolled_back_last_record_lsn_and_ancestor_lsn() {
    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();

    let [later, now, at_restart] = time_backwards();

    // FIXME: tests would be so much easier if we did not need to juggle back and forth
    // SystemTime and DateTime::<Utc> ... Could do the conversion only at upload time?
    let now = DateTime::<Utc>::from(now);
    let later = DateTime::<Utc>::from(later);
    let before_restart = at_restart - std::time::Duration::from_secs(5 * 60);
    let way_before = before_restart - std::time::Duration::from_secs(10 * 60);
    let before_restart = DateTime::<Utc>::from(before_restart);
    let way_before = DateTime::<Utc>::from(way_before);

    let snap = TimelineSnapshot {
        loaded_at: (Lsn(50), at_restart),
        last_record_lsn: Lsn(50),
        ancestor_lsn: Lsn(40),
        current_exact_logical_size: None,
        pitr_enabled: true,
        pitr_cutoff: Some(Lsn(20)),
    };

    let mut cache = HashMap::from([
        MetricsKey::written_size(tenant_id, timeline_id)
            .at(before_restart, 100)
            .to_kv_pair(),
        MetricsKey::written_size_delta(tenant_id, timeline_id)
            .from_until(
                way_before,
                before_restart,
                // not taken into account, but the timestamps are important
                999_999_999,
            )
            .to_kv_pair(),
    ]);

    let mut metrics = Vec::new();
    snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(
                before_restart,
                now,
                0
            ),
            MetricsKey::written_size(tenant_id, timeline_id).at(now, 100),
            MetricsKey::written_size_since_parent(tenant_id, timeline_id).at(now, 60),
            MetricsKey::pitr_history_size_since_parent(tenant_id, timeline_id).at(now, 60),
        ]
    );

    // now if we cache these metrics, and re-run while "still in recovery"
    cache.extend(metrics.drain(..).map(|x| x.to_kv_pair()));

    // "still in recovery", because our snapshot did not change
    snap.to_metrics(tenant_id, timeline_id, later, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(now, later, 0),
            MetricsKey::written_size(tenant_id, timeline_id).at(later, 100),
            MetricsKey::written_size_since_parent(tenant_id, timeline_id).at(later, 60),
            MetricsKey::pitr_history_size_since_parent(tenant_id, timeline_id).at(later, 60),
        ]
    );
}

/// Tests that written sizes do not regress across restarts, even on child branches and
/// with a PITR cutoff after the branch point.
#[test]
fn post_restart_written_sizes_with_rolled_back_last_record_lsn_and_ancestor_lsn_and_pitr_cutoff() {
    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();

    let [later, now, at_restart] = time_backwards();

    // FIXME: tests would be so much easier if we did not need to juggle back and forth
    // SystemTime and DateTime::<Utc> ... Could do the conversion only at upload time?
    let now = DateTime::<Utc>::from(now);
    let later = DateTime::<Utc>::from(later);
    let before_restart = at_restart - std::time::Duration::from_secs(5 * 60);
    let way_before = before_restart - std::time::Duration::from_secs(10 * 60);
    let before_restart = DateTime::<Utc>::from(before_restart);
    let way_before = DateTime::<Utc>::from(way_before);

    let snap = TimelineSnapshot {
        loaded_at: (Lsn(50), at_restart),
        last_record_lsn: Lsn(50),
        ancestor_lsn: Lsn(30),
        current_exact_logical_size: None,
        pitr_enabled: true,
        pitr_cutoff: Some(Lsn(40)),
    };

    let mut cache = HashMap::from([
        MetricsKey::written_size(tenant_id, timeline_id)
            .at(before_restart, 100)
            .to_kv_pair(),
        MetricsKey::written_size_delta(tenant_id, timeline_id)
            .from_until(
                way_before,
                before_restart,
                // not taken into account, but the timestamps are important
                999_999_999,
            )
            .to_kv_pair(),
    ]);

    let mut metrics = Vec::new();
    snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(
                before_restart,
                now,
                0
            ),
            MetricsKey::written_size(tenant_id, timeline_id).at(now, 100),
            MetricsKey::written_size_since_parent(tenant_id, timeline_id).at(now, 70),
            MetricsKey::pitr_history_size_since_parent(tenant_id, timeline_id).at(now, 60),
        ]
    );

    // now if we cache these metrics, and re-run while "still in recovery"
    cache.extend(metrics.drain(..).map(|x| x.to_kv_pair()));

    // "still in recovery", because our snapshot did not change
    snap.to_metrics(tenant_id, timeline_id, later, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(now, later, 0),
            MetricsKey::written_size(tenant_id, timeline_id).at(later, 100),
            MetricsKey::written_size_since_parent(tenant_id, timeline_id).at(later, 70),
            MetricsKey::pitr_history_size_since_parent(tenant_id, timeline_id).at(later, 60),
        ]
    );
}

#[test]
fn post_restart_current_exact_logical_size_uses_cached() {
    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();

    let [now, at_restart] = time_backwards();

    let now = DateTime::<Utc>::from(now);
    let before_restart = at_restart - std::time::Duration::from_secs(5 * 60);
    let before_restart = DateTime::<Utc>::from(before_restart);

    let snap = TimelineSnapshot {
        loaded_at: (Lsn(50), at_restart),
        last_record_lsn: Lsn(50),
        ancestor_lsn: Lsn(0),
        current_exact_logical_size: None,
        pitr_enabled: true,
        pitr_cutoff: None,
    };

    let cache = HashMap::from([MetricsKey::timeline_logical_size(tenant_id, timeline_id)
        .at(before_restart, 100)
        .to_kv_pair()]);

    let mut metrics = Vec::new();
    snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

    metrics.retain(|item| item.key.metric == Name::LogicalSize);

    assert_eq!(
        metrics,
        &[MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, 100)]
    );
}

#[test]
fn post_restart_synthetic_size_uses_cached_if_available() {
    let tenant_id = TenantId::generate();

    let ts = TenantSnapshot {
        remote_size: 1000,
        // not yet calculated
        synthetic_size: 0,
    };

    let now = SystemTime::now();
    let before_restart = DateTime::<Utc>::from(now - std::time::Duration::from_secs(5 * 60));
    let now = DateTime::<Utc>::from(now);

    let cached = HashMap::from([MetricsKey::synthetic_size(tenant_id)
        .at(before_restart, 1000)
        .to_kv_pair()]);

    let mut metrics = Vec::new();
    ts.to_metrics(tenant_id, now, &cached, &mut metrics);

    assert_eq!(
        metrics,
        &[
            MetricsKey::remote_storage_size(tenant_id).at(now, 1000),
            MetricsKey::synthetic_size(tenant_id).at(now, 1000),
        ]
    );
}

#[test]
fn post_restart_synthetic_size_is_not_sent_when_not_cached() {
    let tenant_id = TenantId::generate();

    let ts = TenantSnapshot {
        remote_size: 1000,
        // not yet calculated
        synthetic_size: 0,
    };

    let now = SystemTime::now();
    let now = DateTime::<Utc>::from(now);

    let cached = HashMap::new();

    let mut metrics = Vec::new();
    ts.to_metrics(tenant_id, now, &cached, &mut metrics);

    assert_eq!(
        metrics,
        &[
            MetricsKey::remote_storage_size(tenant_id).at(now, 1000),
            // no synthetic size here
        ]
    );
}

fn time_backwards<const N: usize>() -> [std::time::SystemTime; N] {
    let mut times = [std::time::SystemTime::UNIX_EPOCH; N];
    times[0] = std::time::SystemTime::now();
    for behind in 1..N {
        times[behind] = times[0] - std::time::Duration::from_secs(behind as u64);
    }

    times
}

/// Tests that disabled PITR history does not yield any history size, even when the PITR cutoff
/// indicates otherwise.
#[test]
fn pitr_disabled_yields_no_history_size() {
    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();

    let mut metrics = Vec::new();
    let cache = HashMap::new();

    let initdb_lsn = Lsn(0x10000);
    let pitr_cutoff = Lsn(0x11000);
    let disk_consistent_lsn = Lsn(initdb_lsn.0 * 2);

    let snap = TimelineSnapshot {
        loaded_at: (disk_consistent_lsn, SystemTime::now()),
        last_record_lsn: disk_consistent_lsn,
        ancestor_lsn: Lsn(0),
        current_exact_logical_size: None,
        pitr_enabled: false,
        pitr_cutoff: Some(pitr_cutoff),
    };

    let now = DateTime::<Utc>::from(SystemTime::now());

    snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(
                snap.loaded_at.1.into(),
                now,
                0
            ),
            MetricsKey::written_size(tenant_id, timeline_id).at(now, disk_consistent_lsn.0),
            MetricsKey::written_size_since_parent(tenant_id, timeline_id)
                .at(now, disk_consistent_lsn.0),
            MetricsKey::pitr_history_size_since_parent(tenant_id, timeline_id).at(now, 0),
        ]
    );
}

/// Tests that uninitialized PITR cutoff does not emit any history size metric at all.
#[test]
fn pitr_uninitialized_does_not_emit_history_size() {
    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();

    let mut metrics = Vec::new();
    let cache = HashMap::new();

    let initdb_lsn = Lsn(0x10000);
    let disk_consistent_lsn = Lsn(initdb_lsn.0 * 2);

    let snap = TimelineSnapshot {
        loaded_at: (disk_consistent_lsn, SystemTime::now()),
        last_record_lsn: disk_consistent_lsn,
        ancestor_lsn: Lsn(0),
        current_exact_logical_size: None,
        pitr_enabled: true,
        pitr_cutoff: None,
    };

    let now = DateTime::<Utc>::from(SystemTime::now());

    snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(
                snap.loaded_at.1.into(),
                now,
                0
            ),
            MetricsKey::written_size(tenant_id, timeline_id).at(now, disk_consistent_lsn.0),
            MetricsKey::written_size_since_parent(tenant_id, timeline_id)
                .at(now, disk_consistent_lsn.0),
        ]
    );
}

pub(crate) const fn metric_examples_old(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    now: DateTime<Utc>,
    before: DateTime<Utc>,
) -> [RawMetric; 7] {
    [
        MetricsKey::written_size(tenant_id, timeline_id).at_old_format(now, 0),
        MetricsKey::written_size_delta(tenant_id, timeline_id)
            .from_until_old_format(before, now, 0),
        MetricsKey::written_size_since_parent(tenant_id, timeline_id).at_old_format(now, 0),
        MetricsKey::pitr_history_size_since_parent(tenant_id, timeline_id).at_old_format(now, 0),
        MetricsKey::timeline_logical_size(tenant_id, timeline_id).at_old_format(now, 0),
        MetricsKey::remote_storage_size(tenant_id).at_old_format(now, 0),
        MetricsKey::synthetic_size(tenant_id).at_old_format(now, 1),
    ]
}

pub(crate) const fn metric_examples(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    now: DateTime<Utc>,
    before: DateTime<Utc>,
) -> [NewRawMetric; 7] {
    [
        MetricsKey::written_size(tenant_id, timeline_id).at(now, 0),
        MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(before, now, 0),
        MetricsKey::written_size_since_parent(tenant_id, timeline_id).at(now, 0),
        MetricsKey::pitr_history_size_since_parent(tenant_id, timeline_id).at(now, 0),
        MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, 0),
        MetricsKey::remote_storage_size(tenant_id).at(now, 0),
        MetricsKey::synthetic_size(tenant_id).at(now, 1),
    ]
}

use crate::tenant::timeline::LayerFileName;
use crate::tenant::Timeline;
use std::io::Write;
use std::time::UNIX_EPOCH;
use tracing::*;
use std::fs::File;
use utils::lsn::Lsn;

impl Timeline {

    pub(super) fn start_layer_tracing(&self) {
        let timeline_path = self.conf.timeline_path(&self.timeline_id, &self.tenant_id);

        let path = timeline_path.join("layer_trace");

        match File::options()
            .create(true)
            .append(true)
            .open(&path)
        {
            Ok(file) => {
                info!("enabled layer tracing");
                self.layer_trace_file.lock().unwrap().replace(file);
            },
            Err(e) => {
                warn!("could not open layer tracing file \"{}\": {}", path.display(), e);
            }
        }
    }

    fn trace_op(&self, op: &str, filename: &str) {
        let opt_out = &self.layer_trace_file.lock().unwrap();
        if let Some(mut out) = opt_out.as_ref() {
            if let Ok(elapsed) = UNIX_EPOCH.elapsed() {
                let time = elapsed.as_millis();
                let _ = writeln!(out, "{{ \"time\": {time}, \"op\": \"{op}\", \"filename\": \"{filename}\"}}");
            }
            else {
                warn!("could not get current timestamp");
            }
        }
    }

    pub(super) fn trace_layer_evict(&self, filename: &LayerFileName) {
        self.trace_op("evict", &filename.file_name())
    }

    pub(super) fn trace_layer_flush(&self, filename: &LayerFileName) {
        self.trace_op("flush", &filename.file_name())
    }

    pub(super) fn trace_layer_compact_create(&self, filename: &LayerFileName) {
        self.trace_op("compact_create", &filename.file_name())
    }

    pub(super) fn trace_layer_compact_delete(&self, filename: &LayerFileName) {
        self.trace_op("compact_delete", &filename.file_name())
    }

    pub(super) fn trace_layer_image_create(&self, filename: &LayerFileName) {
        self.trace_op("image_create", &filename.file_name())
    }
    
    pub(super) fn trace_layer_gc_delete(&self, filename: &LayerFileName) {
        self.trace_op("gc_delete", &filename.file_name())
    }

    // TODO: also report 'retain_lsns'
    pub(super) fn trace_gc_start(&self, cutoff_lsn: Lsn) {
        let opt_out = &self.layer_trace_file.lock().unwrap();
        if let Some(mut out) = opt_out.as_ref() {
            if let Ok(elapsed) = UNIX_EPOCH.elapsed() {
                let time = elapsed.as_millis();
                let _ = writeln!(out, "{{ \"time\": {time}, \"op\": \"gc_start\", \"cutoff\": \"{cutoff_lsn}\"}}");
            }
            else {
                warn!("could not get current timestamp");
            }
        }
    }
}

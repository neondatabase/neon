pub trait NotifyImportDoneApi {
    async fn notify_import_done(&self, conf: &PageServerConf) -> Result<(), RetryForeverError>;
}

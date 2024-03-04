use pageserver_compaction::interface::CompactionLayer;
use pageserver_compaction::simulator::MockTimeline;

/// Test the extreme case that there are so many updates for a single key that
/// even if we produce an extremely narrow delta layer, spanning just that one
/// key, we still too many records to fit in the target file size. We need to
/// split in the LSN dimension too in that case.
///
/// TODO: The code to avoid this problem has not been implemented yet! So the
/// assertion currently fails, but we need to make it not fail.
#[ignore]
#[tokio::test]
async fn test_many_updates_for_single_key() {
    let mut executor = MockTimeline::new();
    executor.target_file_size = 10_000_000; // 10 MB

    // Ingest 100 MB of updates to a single key.
    for _ in 1..1000 {
        executor.ingest_uniform(100, 10, &(0..100_000)).unwrap();
        executor.ingest_uniform(10_000, 10, &(0..1)).unwrap();
        executor.compact().await.unwrap();
    }

    // Check that all the layers are smaller than the target size (with some slop)
    for l in executor.live_layers.iter() {
        println!("layer {}: {}", l.short_id(), l.file_size());
    }
    for l in executor.live_layers.iter() {
        assert!(l.file_size() < executor.target_file_size * 2);
        // sanity check that none of the delta layers are stupidly small either
        if l.is_delta() {
            assert!(l.file_size() > executor.target_file_size / 2);
        }
    }
}

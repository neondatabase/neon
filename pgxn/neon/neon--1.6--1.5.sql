DROP FUNCTION IF EXISTS get_prewarm_info(out total_pages integer, out prewarmed_pages integer, out skipped_pages integer, out active_workers integer);

DROP FUNCTION IF EXISTS get_local_cache_state(max_chunks integer);

DROP FUNCTION IF EXISTS prewarm_local_cache(state bytea, n_workers integer);



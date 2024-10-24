DROP FUNCTION IF EXISTS save_local_cache_state();

DROP FUNCTION IF EXISTS get_prewarm_info(out total_chunks integer, out curr_chunk integer, out prewarmed_pages integer, out skipped_pages integer);

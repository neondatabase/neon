-- NOTE: This is the "public" / "human-readable" version. Here, we supply a
-- small selection of durations in a pretty-printed form.

SELECT
  x AS duration,
  COALESCE(neon.approximate_working_set_size_seconds(extract('epoch' FROM x::interval)::int), 0) AS size FROM (
    VALUES ('5m'), ('15m'), ('1h')
  ) AS t (x);

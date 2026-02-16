-- Fix public.customer2 so registration_date accepts Debezium's epoch nanoseconds (bigint)
-- and expose a timestamp via a generated column. Run this on the Postgres cdctest database.
-- Run this BEFORE reprocessing the sink so all 5000+ rows can be inserted.
--
-- For future SQL Server → Postgres pipelines: transfer.py maps datetime2 → bigint, so new
-- tables created by full load will already have datetime2 columns as bigint. Add a generated
-- column per datetime2 column if you want a readable timestamp (see below).

-- Optional: truncate to avoid duplicates when reprocessing from earliest (only if you want a clean reload).
-- TRUNCATE TABLE public.customer2;

-- 1. Change registration_date from timestamp to bigint (raw epoch nanoseconds from SQL Server/Debezium).
--    Existing rows get NULL for registration_date.
ALTER TABLE public.customer2
  ALTER COLUMN registration_date TYPE bigint
  USING NULL;

-- 2. Add a generated column for human-readable timestamp (use for SELECT/display).
ALTER TABLE public.customer2
  ADD COLUMN IF NOT EXISTS registration_ts timestamp without time zone
  GENERATED ALWAYS AS (
    CASE
      WHEN registration_date IS NOT NULL
      THEN to_timestamp(registration_date / 1000000000.0)
      ELSE NULL
    END
  ) STORED;

-- After this, run: python scripts/reprocess_pipeline5_sink.py
-- Then check: SELECT COUNT(*), registration_ts FROM public.customer2 GROUP BY registration_ts;

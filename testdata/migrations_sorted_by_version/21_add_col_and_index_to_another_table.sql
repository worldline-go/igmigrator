ALTER TABLE another ADD purchased_at timestamptz;
CREATE INDEX ON another (purchased_at)

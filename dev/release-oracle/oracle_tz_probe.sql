-- Gate-owned Oracle fixture, appended to seeds/common/oracle.sql in the gate's OWN
-- container only (dev/release_oracle/__main__.py::seed_engine). The canonical seed's
-- rivet_type_matrix has no TIMESTAMP WITH TIME ZONE column, so without this table
-- integrity_types[oracle] cannot see the offset / region-name handling at all.
DROP TABLE IF EXISTS rivet_tz_probe PURGE;
CREATE TABLE rivet_tz_probe (
    id    NUMBER(10) PRIMARY KEY,
    tstz  TIMESTAMP(6) WITH TIME ZONE,
    tsltz TIMESTAMP(6) WITH LOCAL TIME ZONE
);
INSERT INTO rivet_tz_probe (id, tstz, tsltz) VALUES
  (1, TIMESTAMP '2024-02-29 10:00:00.123456 +02:00', TIMESTAMP '2024-02-29 10:00:00.5 -03:00'),
  (2, TO_TIMESTAMP_TZ('2024-07-01 10:00:00 Europe/Berlin', 'YYYY-MM-DD HH24:MI:SS TZR'),
      TIMESTAMP '2024-07-01 10:00:00 +09:00'),
  (3, TO_TIMESTAMP_TZ('2024-01-15 08:30:00.000001 America/New_York', 'YYYY-MM-DD HH24:MI:SS.FF TZR'),
      TIMESTAMP '2024-01-15 08:30:00 +00:00'),
  (4, NULL, NULL),
  (5, TIMESTAMP '2024-12-31 23:30:00 -05:00', TIMESTAMP '2025-01-01 00:30:00 +05:30');
COMMIT;

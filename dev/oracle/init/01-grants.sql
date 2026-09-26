-- Run once by the image's first start (as SYSDBA, in CDB$ROOT).
-- rivet reads row estimates (ALL_TABLES) and source-harm counters (V$SYSSTAT,
-- V$SYSTEM_EVENT); SELECT_CATALOG_ROLE covers both for the test stand's user.
ALTER SESSION SET CONTAINER = FREEPDB1;
GRANT SELECT_CATALOG_ROLE TO rivet;
GRANT CREATE VIEW, CREATE SEQUENCE, CREATE PROCEDURE TO rivet;

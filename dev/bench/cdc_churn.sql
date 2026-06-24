-- Continuous OLTP churn generator for CDC harm/perf testing.
--
-- `cdc_churn(secs)` runs flat-out for `secs` seconds, each iteration doing one
-- realistic single-row UPDATE on content_items (by PK — a view bump + status +
-- updated_at) and one INSERT into page_views (a logged page view). With
-- autocommit on, each statement is its own transaction → its own binlog XID, so a
-- `rivet cdc` reader sees them as individual change events.
--
-- Usage:
--   docker exec -i rivet-mysql-1 mysql -urivet -privet rivet < dev/bench/cdc_churn.sql
--   docker exec rivet-mysql-1 mysql -urivet -privet rivet -e "CALL cdc_churn(20)"

DROP PROCEDURE IF EXISTS cdc_churn;

DELIMITER $$
CREATE PROCEDURE cdc_churn(IN secs INT)
BEGIN
    -- SYSDATE() (not NOW()) — NOW() is frozen at the CALL start inside a
    -- procedure, so a NOW()-based deadline would never trip.
    DECLARE deadline DATETIME DEFAULT (SYSDATE() + INTERVAL secs SECOND);
    DECLARE maxid BIGINT DEFAULT (SELECT MAX(id) FROM content_items);
    DECLARE tid BIGINT;
    WHILE SYSDATE() < deadline DO
        -- Pick the random id into a variable FIRST, so the UPDATE's `WHERE id =
        -- tid` is a primary-key lookup. `WHERE id = 1+FLOOR(RAND()*maxid)`
        -- re-evaluates RAND() per row → a full table scan; the variable avoids it.
        SET tid = 1 + FLOOR(RAND() * maxid);
        -- "SILENT" update — changes a value but does NOT touch updated_at.
        -- A watermark/incremental sync (`WHERE updated_at > last_seen`) MISSES
        -- this row; log-based CDC catches it because the binlog records the
        -- actual row change regardless of which columns moved. This is the case
        -- that breaks watermark replication and that CDC exists to solve.
        UPDATE content_items SET view_count = view_count + 1 WHERE id = tid;

        -- "TRACKED" update — bumps updated_at (a watermark sync WOULD see this).
        SET tid = 1 + FLOOR(RAND() * maxid);
        UPDATE content_items
           SET status = ELT(1 + FLOOR(RAND() * 3), 'draft', 'published', 'archived'),
               updated_at = NOW()
         WHERE id = tid;

        -- a logged page view (append-only insert)
        INSERT INTO page_views (session_id, user_id, url, device_type, browser, page_load_ms)
        VALUES (UUID(),
                FLOOR(RAND() * 100000),
                CONCAT('/article/', 1 + FLOOR(RAND() * maxid)),
                ELT(1 + FLOOR(RAND() * 3), 'desktop', 'mobile', 'tablet'),
                ELT(1 + FLOOR(RAND() * 3), 'chrome', 'firefox', 'safari'),
                FLOOR(RAND() * 2000));
    END WHILE;
END$$
DELIMITER ;

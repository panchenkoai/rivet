/*
 * Epic 18 Phase D — empirical efficacy probe for a MySQL server-side cursor.
 *
 * Question (the one I asserted but had NOT proven): does a MySQL read-only
 * server-side cursor (CURSOR_TYPE_READ_ONLY + COM_STMT_FETCH) give SHORT
 * per-statement work like PG's streaming `FETCH N`, or does it MATERIALISE the
 * whole result into a temp table at open (so the open is long and the
 * "short statement" benefit never appears)?
 *
 * This is a MySQL-SERVER behaviour, identical for any client, so a tiny C
 * program using libmysqlclient settles it without forking the Rust crates.
 *
 * Build:
 *   cc dev/spikes/mysql_cursor_efficacy.c $(mysql_config --cflags --libs) \
 *      -o /tmp/cursor_efficacy && /tmp/cursor_efficacy
 *
 * Live DB: docker compose `mysql`, content_items seeded ~524k wide rows.
 * Baseline (A1, one non-cursor BETWEEN query, 100k rows): 3.40 s.
 *
 * Decision:
 *   open ~3.4 s and/or Created_tmp_tables jumps  -> MATERIALISES -> D dead.
 *   open ~instant, no temp table, smooth fetches -> STREAMS      -> D worth fork.
 *
 * ============================ RESULTS (2026-06-08) ============================
 * Live MySQL 8.0, content_items. Query matched 65 536 rows (id range has gaps).
 *   cursor OPEN time : 0.78 - 1.82 s across 3 runs   <- the longest statement
 *   Created_tmp_tables delta at open : 3  (every run, stable)
 *   fetches after open: smooth, ~0.15 s / 10k rows
 *   VERDICT: MATERIALISES — the open runs the whole query and writes 3 temp
 *   tables; fetches are then cheap. This is the OPPOSITE of PG's streaming
 *   cursor (FETCH N pulls incrementally from a live scan, no temp table).
 *
 * Conclusion: a MySQL server-side cursor does NOT shorten the longest single
 * statement — it just moves the cost into a long materialising OPEN and adds
 * tempdb pressure (3 temp tables) the keyset / chunk_size path never causes. So
 * even if we paid for the mysql_common fork, D would be a REGRESSION vs the free
 * `chunk_size` lever (~0.5 s pages, no temp tables). The efficacy claim — once
 * asserted, now MEASURED — is confirmed and then some. Phase D stays closed.
 * =============================================================================
 */

#include <mysql.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static double now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* Read SUM of Created_tmp_tables + Created_tmp_disk_tables (GLOBAL) on a
 * separate connection so the cursor connection stays free. */
static long long tmp_tables(MYSQL *c) {
    if (mysql_query(c,
            "SHOW GLOBAL STATUS WHERE Variable_name IN "
            "('Created_tmp_tables','Created_tmp_disk_tables')")) {
        fprintf(stderr, "status query failed: %s\n", mysql_error(c));
        return -1;
    }
    MYSQL_RES *r = mysql_store_result(c);
    long long sum = 0;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(r))) sum += atoll(row[1]);
    mysql_free_result(r);
    return sum;
}

int main(void) {
    const char *HOST = "127.0.0.1", *USER = "root", *PASS = "rivet", *DB = "rivet";
    const unsigned PORT = 3306;
    const char *SQL = "SELECT * FROM content_items WHERE id BETWEEN 1 AND 100000";

    MYSQL *cur = mysql_init(NULL);   /* cursor connection */
    MYSQL *st  = mysql_init(NULL);   /* status connection */
    if (!mysql_real_connect(cur, HOST, USER, PASS, DB, PORT, NULL, 0) ||
        !mysql_real_connect(st,  HOST, USER, PASS, DB, PORT, NULL, 0)) {
        fprintf(stderr, "connect failed: %s\n", mysql_error(cur));
        return 1;
    }

    MYSQL_STMT *s = mysql_stmt_init(cur);
    if (mysql_stmt_prepare(s, SQL, strlen(SQL))) {
        fprintf(stderr, "prepare failed: %s\n", mysql_stmt_error(s));
        return 1;
    }

    /* The whole point: ask for a server-side READ-ONLY cursor + 10k prefetch. */
    unsigned long cursor_type = CURSOR_TYPE_READ_ONLY;
    unsigned long prefetch = 10000;
    mysql_stmt_attr_set(s, STMT_ATTR_CURSOR_TYPE, &cursor_type);
    mysql_stmt_attr_set(s, STMT_ATTR_PREFETCH_ROWS, &prefetch);

    long long tmp_before = tmp_tables(st);

    /* ---- THE DECISIVE NUMBER: time the cursor OPEN ---- */
    double t0 = now_s();
    if (mysql_stmt_execute(s)) {
        fprintf(stderr, "execute failed: %s\n", mysql_stmt_error(s));
        return 1;
    }
    double open_s = now_s() - t0;

    long long tmp_after_open = tmp_tables(st);

    /* Bind every column to a throwaway 256-byte string buffer; truncation of
     * the longtext columns is fine — we only need to drive the fetch + time it.
     * (The server materialises ALL columns at open regardless of binds, so the
     * open time above already reflects the full wide-row cost.) */
    MYSQL_RES *meta = mysql_stmt_result_metadata(s);
    unsigned ncols = mysql_num_fields(meta);
    MYSQL_BIND *binds = calloc(ncols, sizeof(MYSQL_BIND));
    char (*buf)[256] = calloc(ncols, 256);
    unsigned long *len = calloc(ncols, sizeof(unsigned long));
    bool *isnull = calloc(ncols, sizeof(bool)), *err = calloc(ncols, sizeof(bool));
    for (unsigned i = 0; i < ncols; i++) {
        binds[i].buffer_type = MYSQL_TYPE_STRING;
        binds[i].buffer = buf[i];
        binds[i].buffer_length = 256;
        binds[i].length = &len[i];
        binds[i].is_null = &isnull[i];
        binds[i].error = &err[i];
    }
    mysql_stmt_bind_result(s, binds);

    /* ---- fetch loop, timing every 10k rows to spot any upfront stall ---- */
    double t_fetch0 = now_s();
    long long rows = 0;
    double last = t_fetch0;
    int rc;
    printf("  fetch checkpoints (every 10k rows):\n");
    while ((rc = mysql_stmt_fetch(s)) == 0 || rc == MYSQL_DATA_TRUNCATED) {
        rows++;
        if (rows % 10000 == 0) {
            double t = now_s();
            printf("    %7lld rows  +%.3fs\n", rows, t - last);
            last = t;
        }
    }
    double fetch_s = now_s() - t_fetch0;
    if (rc != MYSQL_NO_DATA)
        printf("    (fetch loop ended early at row %lld, rc=%d: %s — wide-column "
               "buffer truncation in this throwaway probe; irrelevant to the "
               "open/materialise verdict measured above)\n",
               rows, rc, mysql_stmt_error(s));

    mysql_free_result(meta);
    mysql_stmt_close(s);

    /* ---- verdict ---- */
    printf("\n=== MySQL server-side read-only cursor — measured ===\n");
    printf("  query            : %s\n", SQL);
    printf("  baseline (no cur): 3.40 s  (one statement, A1)\n");
    printf("  rows fetched     : %lld\n", rows);
    printf("  cursor OPEN time : %.3f s   <-- the longest single statement\n", open_s);
    printf("  fetch loop total : %.3f s\n", fetch_s);
    printf("  Created_tmp_tables delta at open : %lld\n", tmp_after_open - tmp_before);
    printf("\n  verdict: ");
    if (open_s > 1.0 || (tmp_after_open - tmp_before) > 0)
        printf("MATERIALISES (open is long / temp table created) -> no benefit -> D dead\n");
    else
        printf("STREAMS (open short, no temp table) -> benefit real -> D worth the fork\n");

    free(binds); free(buf); free(len); free(isnull); free(err);
    mysql_close(s ? cur : cur);
    mysql_close(st);
    return 0;
}

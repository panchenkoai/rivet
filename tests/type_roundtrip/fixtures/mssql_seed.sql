-- Three rows: a fully-populated row, a negative/boundary row, and a row that
-- exercises the nullable columns (note_all_null is NULL in every row).
INSERT INTO {table_name}
    (id, c_smallint, c_int, c_bigint, c_tinyint, c_bit, amount, fee, price,
     c_real, c_float, c_date, c_time, created_at, label, c_varchar, c_char,
     raw_bytes, uid, c_nvarchar, note_nullable, note_all_null)
VALUES
    (1, 30000, 2000000000, 9000000000, 200, 1, 1234.56, 0.000001, 9999.99,
     2.5, 3.141592653589793, '2026-01-15', '13:45:30.123456',
     '2026-01-15T13:45:30.123456', N'first label', 'ascii-vc', 'fixedchar',
     0x00112233445566FF, '6F9619FF-8B86-D011-B42D-00C04FC964FF',
     N'héllo wörld', N'present', NULL),
    (2, -30000, -2000000000, -9000000000, 0, 0, -0.01, -0.000001, -0.01,
     -0.5, -2.5, '1999-12-31', '00:00:00.000000',
     '2000-01-01T00:00:00', N'second', 'vc2', 'c2',
     0xDEADBEEF, '00000000-0000-0000-0000-000000000001',
     N'unicode: ✓ 日本語', NULL, NULL),
    (3, 0, 0, 0, 255, 1, 0.00, 0.000000, 0.00,
     0.0, 0.0, '2026-06-05', '23:59:59.999999',
     '2026-06-05T23:59:59.999999', N'third', 'vc3', 'c3',
     0x00, '11111111-2222-3333-4444-555555555555',
     N'plain', N'also present', NULL);

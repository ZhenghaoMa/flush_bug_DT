SELECT iceberg_unmount('ft1');
SELECT iceberg_mount('public', 'ft1', '/tmp/test_iceberg/v3_min');
SELECT count(*) FROM ft1;

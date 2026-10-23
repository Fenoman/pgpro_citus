--
-- COLUMNAR_PARTITIONED_AM
-- Since PG17 a partitioned table can carry a table access method. It has no
-- storage of its own, only its partitions are columnar tables.
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 17 AS server_version_ge_17
\gset
\if :server_version_ge_17
\else
\q
\endif

CREATE SCHEMA columnar_partitioned_am;
SET search_path TO columnar_partitioned_am, public;

CREATE TABLE parent (id int, payload text) PARTITION BY RANGE (id) USING columnar;
CREATE TABLE child_1 PARTITION OF parent FOR VALUES FROM (0) TO (100);
CREATE TABLE child_2 PARTITION OF parent FOR VALUES FROM (100) TO (200);
SELECT c.relname, c.relkind, am.amname
    FROM pg_class c JOIN pg_am am ON am.oid = c.relam
    WHERE c.relnamespace = 'columnar_partitioned_am'::regnamespace
    ORDER BY c.relname;
INSERT INTO parent SELECT i, 'row ' || i FROM generate_series(0, 199) i;
SELECT count(*), sum(id) FROM parent;

-- Only the partitions are columnar tables.
SELECT relation FROM columnar.options
    WHERE relation IN ('parent'::regclass, 'child_1'::regclass, 'child_2'::regclass)
    ORDER BY relation;
SELECT version_major FROM columnar_test_helpers.columnar_storage_info('child_1');
SELECT version_major FROM columnar_test_helpers.columnar_storage_info('parent');

-- Dropping the partitioned table removes the columnar metadata of its partitions.
SELECT 'child_1'::regclass::oid AS child_1_oid, 'child_2'::regclass::oid AS child_2_oid \gset
SELECT storage_id AS child_1_storage_id
    FROM columnar_test_helpers.columnar_storage_info('child_1') \gset
DROP TABLE parent;
SELECT count(*) AS leftover_options FROM columnar_internal.options
    WHERE regclass::oid IN (:child_1_oid, :child_2_oid);
SELECT count(*) AS leftover_stripes FROM columnar_internal.stripe
    WHERE storage_id = :child_1_storage_id;

-- A partitioned heap table switched to columnar keeps its existing partitions
-- and creates new ones as columnar.
CREATE TABLE converted (id int) PARTITION BY RANGE (id);
CREATE TABLE converted_heap PARTITION OF converted FOR VALUES FROM (0) TO (100);
ALTER TABLE converted SET ACCESS METHOD columnar;
CREATE TABLE converted_columnar PARTITION OF converted FOR VALUES FROM (100) TO (200);
SELECT c.relname, c.relkind, am.amname
    FROM pg_class c JOIN pg_am am ON am.oid = c.relam
    WHERE c.relname LIKE 'converted%'
    ORDER BY c.relname;
INSERT INTO converted SELECT generate_series(0, 199);
SELECT count(*) FROM converted;
DROP TABLE converted;

SET client_min_messages TO warning;
DROP SCHEMA columnar_partitioned_am CASCADE;
RESET client_min_messages;
RESET search_path;

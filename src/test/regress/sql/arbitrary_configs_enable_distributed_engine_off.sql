SET client_min_messages TO WARNING;
CREATE EXTENSION IF NOT EXISTS citus;
CREATE EXTENSION IF NOT EXISTS citus_columnar;
CREATE SCHEMA IF NOT EXISTS arbitrary_configs_engine_off;
CREATE SCHEMA IF NOT EXISTS tenant_attempt;
SET search_path TO arbitrary_configs_engine_off;

DROP TABLE IF EXISTS single_attempt;
DROP TABLE IF EXISTS local_attempt;
DROP TABLE IF EXISTS ref_attempt;
DROP TABLE IF EXISTS dist_concurrent_attempt;
DROP TABLE IF EXISTS dist_attempt;
DROP TABLE IF EXISTS col_b;
DROP TABLE IF EXISTS col_a;
DROP TABLE IF EXISTS reg_b;
DROP TABLE IF EXISTS reg_a;

CREATE TABLE reg_a(id int PRIMARY KEY, grp int UNIQUE, payload text);
CREATE TABLE reg_b(id int PRIMARY KEY, grp int UNIQUE, payload text);
INSERT INTO reg_a VALUES (1, 10, 'aa'), (2, 20, 'bb'), (3, 30, 'cc');
INSERT INTO reg_b VALUES (10, 10, 'xa'), (20, 30, 'xb'), (30, 40, 'xc');

CREATE TABLE col_a(id int, grp int, payload text) USING columnar;
CREATE TABLE col_b(id int, grp int, payload text) USING columnar;
INSERT INTO col_a VALUES (1, 10, 'ca'), (2, 20, 'cb'), (3, 40, 'cc');
INSERT INTO col_b VALUES (10, 10, 'da'), (20, 40, 'db'), (30, 50, 'dc');

CREATE TABLE dist_attempt(id int, payload text);
CREATE TABLE dist_concurrent_attempt(id int, payload text);
CREATE TABLE ref_attempt(id int, payload text);
CREATE TABLE local_attempt(id int, payload text);
CREATE TABLE single_attempt(id int, payload text);
CREATE OR REPLACE FUNCTION noop_add(i int) RETURNS int
LANGUAGE SQL
AS $$ SELECT i $$;

SELECT count(*) FROM reg_a INNER JOIN reg_b USING (grp);
SELECT count(*) FROM reg_a LEFT JOIN reg_b USING (grp);
SELECT count(*) FROM reg_a RIGHT JOIN reg_b USING (grp);
SELECT count(*) FROM reg_a FULL JOIN reg_b USING (grp);

SELECT count(*) FROM col_a INNER JOIN col_b USING (grp);
SELECT count(*) FROM col_a LEFT JOIN col_b USING (grp);
SELECT count(*) FROM col_a RIGHT JOIN col_b USING (grp);
SELECT count(*) FROM col_a FULL JOIN col_b USING (grp);

SELECT count(*) FROM reg_a INNER JOIN col_a USING (grp);
SELECT count(*) FROM reg_a LEFT JOIN col_a USING (grp);
SELECT count(*) FROM reg_a RIGHT JOIN col_a USING (grp);
SELECT count(*) FROM reg_a FULL JOIN col_a USING (grp);

SELECT count(*)
FROM reg_a
FULL JOIN reg_b USING (grp)
FULL JOIN col_a USING (grp)
FULL JOIN col_b USING (grp);

UPDATE reg_a SET payload = 'BB' WHERE grp = 20;
SELECT payload FROM reg_a WHERE grp = 20;

DELETE FROM reg_b WHERE grp = 40;
SELECT count(*) FROM reg_b;

\set VERBOSITY terse
DELETE FROM col_a WHERE grp = 20;
\set VERBOSITY default

TRUNCATE col_b;
INSERT INTO col_b VALUES (40, 60, 'reloaded');
SELECT count(*) FROM col_b;

\set VERBOSITY terse
SELECT create_distributed_table('dist_attempt', 'id');
SELECT create_distributed_table_concurrently('dist_concurrent_attempt', 'id');
SELECT create_reference_table('ref_attempt');
SELECT citus_add_local_table_to_metadata('local_attempt');
SELECT create_distributed_table('single_attempt', NULL);
SELECT citus_set_coordinator_host('localhost');
SELECT master_add_node('localhost', 5432);
SELECT citus_update_node(1, 'localhost', 5432);
SELECT citus_pause_node_within_txn(1);
SELECT create_distributed_function('arbitrary_configs_engine_off.noop_add(int)'::regprocedure,
                                   NULL::text,
                                   'default');
SELECT undistribute_table('arbitrary_configs_engine_off.dist_attempt'::regclass, false);
SELECT alter_distributed_table('arbitrary_configs_engine_off.dist_attempt'::regclass,
                               'id',
                               NULL::integer,
                               NULL::text,
                               NULL::boolean);
SELECT update_distributed_table_colocation('arbitrary_configs_engine_off.dist_attempt'::regclass,
                                           'none');
SELECT replicate_reference_tables('auto'::citus.shard_transfer_mode);
SELECT citus_rebalance_start();
SELECT * FROM get_rebalance_progress();
SELECT master_create_empty_shard('arbitrary_configs_engine_off.dist_attempt');
SELECT citus_schema_distribute('tenant_attempt'::regnamespace);
SELECT citus_split_shard_by_split_points(1,
                                         ARRAY['10'],
                                         ARRAY[1],
                                         'auto'::citus.shard_transfer_mode);
CALL citus_cleanup_orphaned_resources();
SELECT citus_unmark_object_distributed('pg_class'::regclass,
                                       'arbitrary_configs_engine_off.reg_a'::regclass,
                                       0,
                                       false);
\set VERBOSITY default

SELECT count(*) FROM pg_catalog.pg_dist_partition;

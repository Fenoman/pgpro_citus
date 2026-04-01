SET client_min_messages TO WARNING;

DROP SCHEMA IF EXISTS arbitrary_configs_engine_off CASCADE;
CREATE SCHEMA arbitrary_configs_engine_off;
SET search_path TO arbitrary_configs_engine_off;

SELECT current_setting('citus.enable_distributed_engine');

SELECT count(*) FROM pg_extension WHERE extname = 'citus';
DROP EXTENSION citus;
SELECT count(*) FROM pg_extension WHERE extname = 'citus';
CREATE EXTENSION citus;
CREATE EXTENSION IF NOT EXISTS citus;
CREATE EXTENSION citus_columnar;
SELECT count(*) FROM pg_extension WHERE extname IN ('citus', 'citus_columnar');

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

SELECT count(*) FROM pg_catalog.pg_dist_partition;

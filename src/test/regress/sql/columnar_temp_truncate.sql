--
-- COLUMNAR_TEMP_TRUNCATE
-- A nontransactional truncate of a temporary columnar table starts a new
-- storage. PostgresPro Enterprise may drop the local buffers of a temporary
-- table with indexes before the truncate, and the metapage read during the
-- truncate must not survive it.
--
CREATE SCHEMA columnar_temp_truncate;
SET search_path TO columnar_temp_truncate, public;

-- TRUNCATE in the transaction that created the table
BEGIN;
CREATE TEMP TABLE truncated_in_creating_tx (a int PRIMARY KEY, b text) USING columnar;
INSERT INTO truncated_in_creating_tx SELECT g, repeat('x', 3000) FROM generate_series(1, 2000) g;
SELECT columnar.get_storage_id('truncated_in_creating_tx') AS storage_id_before \gset
TRUNCATE truncated_in_creating_tx;
SELECT columnar.get_storage_id('truncated_in_creating_tx') <> :storage_id_before AS storage_id_changed;
INSERT INTO truncated_in_creating_tx SELECT g, 'y' FROM generate_series(1, 3) g;
COMMIT;
SELECT count(*), sum(a) FROM truncated_in_creating_tx;
SELECT count(*) AS stripes_of_old_storage FROM columnar_internal.stripe
    WHERE storage_id = :storage_id_before;

-- ON COMMIT DELETE ROWS, two cycles
CREATE TEMP TABLE deleted_on_commit (a int PRIMARY KEY, b text) USING columnar ON COMMIT DELETE ROWS;
BEGIN;
INSERT INTO deleted_on_commit SELECT g, repeat('x', 3000) FROM generate_series(1, 2000) g;
COMMIT;
BEGIN;
INSERT INTO deleted_on_commit SELECT g, repeat('x', 3000) FROM generate_series(1, 2000) g;
SELECT columnar.get_storage_id('deleted_on_commit') AS storage_id_in_tx \gset
COMMIT;
SELECT columnar.get_storage_id('deleted_on_commit') <> :storage_id_in_tx AS storage_id_changed;
SELECT count(*) AS stripes_of_old_storage FROM columnar_internal.stripe
    WHERE storage_id = :storage_id_in_tx;
BEGIN;
INSERT INTO deleted_on_commit SELECT g, 'y' FROM generate_series(1, 5) g;
SELECT count(*), sum(a) FROM deleted_on_commit;
COMMIT;
SELECT count(*) FROM deleted_on_commit;

DROP TABLE truncated_in_creating_tx, deleted_on_commit;
SET client_min_messages TO warning;
DROP SCHEMA columnar_temp_truncate CASCADE;
RESET client_min_messages;
RESET search_path;

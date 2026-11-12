--
-- COLUMNAR_AUTONOMOUS
-- An autonomous transaction of PostgresPro Enterprise shares the columnar write
-- state of the transaction it suspends, so citus_columnar and citus make the
-- server refuse autonomous transactions. The test harness preloads citus, and
-- its mark alone is enough for the refusal here. Other servers have no
-- autonomous transactions and only keep the buffered row.
--
CREATE SCHEMA columnar_autonomous;
SET search_path TO columnar_autonomous, public;
SET lc_messages TO 'C';

CREATE TABLE buffered (a int) USING columnar;
CREATE TABLE attempt (refused bool);

-- enable_atx exists only on a server with autonomous transactions
SELECT current_setting('enable_atx', true) IS NOT NULL AS has_atx \gset

\if :has_atx
CREATE FUNCTION autonomous_insert() RETURNS void LANGUAGE plpgsql AS $$
BEGIN AUTONOMOUS
    INSERT INTO columnar_autonomous.buffered VALUES (2);
END
$$;
BEGIN;
INSERT INTO buffered VALUES (1);
DO $$
BEGIN
    PERFORM columnar_autonomous.autonomous_insert();
    INSERT INTO columnar_autonomous.attempt VALUES (false);
EXCEPTION WHEN OTHERS THEN
    INSERT INTO columnar_autonomous.attempt
        VALUES (SQLERRM LIKE '% extension is incompatible with autonomous transactions');
END
$$;
COMMIT;
\else
INSERT INTO buffered VALUES (1);
INSERT INTO attempt VALUES (true);
\endif

SELECT refused FROM attempt;
SELECT a FROM buffered ORDER BY a;

SET client_min_messages TO warning;
DROP SCHEMA columnar_autonomous CASCADE;
RESET client_min_messages;
RESET lc_messages;
RESET search_path;

use strict;
use warnings;

my $pg_major_version = int($ENV{'pg_major_version'} || 0);
if ($pg_major_version >= 15)
{
	eval "use PostgreSQL::Test::Cluster";
	eval "use PostgreSQL::Test::Utils";
}
else
{
	eval "use PostgresNode";
}

use Test::More;

my @nodes_to_cleanup = ();

END
{
	foreach my $node (@nodes_to_cleanup)
	{
		eval { $node->stop('fast'); };
	}
}

sub create_single_node
{
	my ($name, $config) = @_;
	my $node;

	if ($pg_major_version >= 15)
	{
		$PostgreSQL::Test::Cluster::use_unix_sockets = 0;
		$PostgreSQL::Test::Cluster::use_tcp = 1;
		$PostgreSQL::Test::Cluster::test_pghost = 'localhost';
		$node = PostgreSQL::Test::Cluster->new($name, host => 'localhost');
	}
	else
	{
		$PostgresNode::use_tcp = 1;
		$PostgresNode::test_pghost = '127.0.0.1';
		$node = get_new_node($name, host => 'localhost');
	}

	my $postgresql_conf = <<'CONF';
shared_preload_libraries = 'citus'
max_connections = 100
max_wal_senders = 10
max_replication_slots = 10
log_statement = 'all'
ssl = off
CONF

	$postgresql_conf .= $config if defined($config);

	$node->init(allows_streaming => 'logical');
	$node->append_conf('postgresql.conf', $postgresql_conf);
	$node->start();

	push @nodes_to_cleanup, $node;

	return $node;
}

sub fails_like_sql
{
	my ($node, $dbname, $sql, $pattern, $test_name) = @_;

	my $ok = eval
	{
		$node->safe_psql($dbname, $sql);
		1;
	};

	ok(!$ok, $test_name);
	like($@, $pattern, "$test_name reports the expected error");
}

subtest 'engine off lifecycle and columnar-only behavior' => sub
{
	my $node = create_single_node(
		'engine_off',
		"citus.enable_distributed_engine = off\n");

	is($node->safe_psql('postgres',
						"SELECT current_setting('citus.enable_distributed_engine');"),
	   'off',
	   'postmaster GUC is off');

	$node->safe_psql('postgres', 'CREATE DATABASE lifecycle_db;');
	$node->safe_psql('postgres', 'CREATE DATABASE columnar_db;');

	$node->safe_psql('lifecycle_db', 'CREATE EXTENSION citus;');
	is($node->safe_psql('lifecycle_db',
						"SELECT count(*) FROM pg_extension WHERE extname = 'citus';"),
	   '1',
	   'CREATE EXTENSION citus works when the engine is off');

	ok(eval { $node->safe_psql('lifecycle_db', 'CREATE EXTENSION IF NOT EXISTS citus;'); 1; },
	   'CREATE EXTENSION IF NOT EXISTS citus succeeds when already installed');

	$node->safe_psql('lifecycle_db', 'DROP EXTENSION citus;');
	is($node->safe_psql('lifecycle_db',
						"SELECT count(*) FROM pg_extension WHERE extname = 'citus';"),
	   '0',
	   'DROP EXTENSION citus works when the engine is off');

	$node->safe_psql('columnar_db', q{
		CREATE EXTENSION citus;
		CREATE EXTENSION citus_columnar;
		CREATE SCHEMA tenant_attempt;

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
		CREATE FUNCTION noop_add(i int) RETURNS int
		LANGUAGE SQL
		AS $$ SELECT i $$;
	});

	is($node->safe_psql('columnar_db',
						"SELECT count(*) FROM pg_extension WHERE extname IN ('citus', 'citus_columnar');"),
	   '2',
	   'citus and citus_columnar are installed together');

	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM pg_catalog.pg_dist_partition;'),
	   '0',
	   'columnar-only setup does not create distributed metadata');

	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM reg_a INNER JOIN reg_b USING (grp);'),
	   '2',
	   'regular INNER JOIN works');
	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM reg_a LEFT JOIN reg_b USING (grp);'),
	   '3',
	   'regular LEFT JOIN works');
	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM reg_a RIGHT JOIN reg_b USING (grp);'),
	   '3',
	   'regular RIGHT JOIN works');
	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM reg_a FULL JOIN reg_b USING (grp);'),
	   '4',
	   'regular FULL JOIN works');

	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM col_a INNER JOIN col_b USING (grp);'),
	   '2',
	   'columnar INNER JOIN works');
	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM col_a LEFT JOIN col_b USING (grp);'),
	   '3',
	   'columnar LEFT JOIN works');
	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM col_a RIGHT JOIN col_b USING (grp);'),
	   '3',
	   'columnar RIGHT JOIN works');
	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM col_a FULL JOIN col_b USING (grp);'),
	   '4',
	   'columnar FULL JOIN works');

	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM reg_a INNER JOIN col_a USING (grp);'),
	   '2',
	   'mixed INNER JOIN works');
	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM reg_a LEFT JOIN col_a USING (grp);'),
	   '3',
	   'mixed LEFT JOIN works');
	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM reg_a RIGHT JOIN col_a USING (grp);'),
	   '3',
	   'mixed RIGHT JOIN works');
	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM reg_a FULL JOIN col_a USING (grp);'),
	   '4',
	   'mixed FULL JOIN works');
	is($node->safe_psql('columnar_db', q{
		SELECT count(*)
		FROM reg_a
		FULL JOIN reg_b USING (grp)
		FULL JOIN col_a USING (grp)
		FULL JOIN col_b USING (grp);
	}),
	   '5',
	   'joining all regular and columnar tables works');

	$node->safe_psql('columnar_db',
					 "UPDATE reg_a SET payload = 'BB' WHERE grp = 20;");
	is($node->safe_psql('columnar_db',
						'SELECT payload FROM reg_a WHERE grp = 20;'),
	   'BB',
	   'UPDATE works on regular tables');

	$node->safe_psql('columnar_db',
					 'DELETE FROM reg_b WHERE grp = 40;');
	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM reg_b;'),
	   '2',
	   'DELETE works on regular tables');

	fails_like_sql(
		$node,
		'columnar_db',
		'DELETE FROM col_a WHERE grp = 20;',
		qr/UPDATE and CTID scans not supported for ColumnarScan/,
		'DELETE from a columnar table is rejected');

	$node->safe_psql('columnar_db',
					 "TRUNCATE col_b; INSERT INTO col_b VALUES (40, 60, 'reloaded');");
	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM col_b;'),
	   '1',
	   'TRUNCATE works on columnar tables');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT create_distributed_table('dist_attempt', 'id');",
		qr/cannot create distributed tables when citus\.enable_distributed_engine is disabled/,
		'create_distributed_table is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT create_distributed_table_concurrently('dist_concurrent_attempt', 'id');",
		qr/cannot distribute tables concurrently when citus\.enable_distributed_engine is disabled/,
		'create_distributed_table_concurrently is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT create_reference_table('ref_attempt');",
		qr/cannot create reference tables when citus\.enable_distributed_engine is disabled/,
		'create_reference_table is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT citus_add_local_table_to_metadata('local_attempt');",
		qr/cannot create Citus local tables when citus\.enable_distributed_engine is disabled/,
		'citus_add_local_table_to_metadata is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT create_distributed_table('single_attempt', NULL);",
		qr/cannot create distributed tables when citus\.enable_distributed_engine is disabled/,
		'single-shard distributed-table creation is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT citus_set_coordinator_host('localhost');",
		qr/cannot manage Citus node metadata when citus\.enable_distributed_engine is disabled/,
		'citus_set_coordinator_host is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT master_add_node('localhost', 5432);",
		qr/cannot manage Citus node metadata when citus\.enable_distributed_engine is disabled/,
		'master_add_node is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT citus_update_node(1, 'localhost', 5432);",
		qr/cannot manage Citus node metadata when citus\.enable_distributed_engine is disabled/,
		'citus_update_node is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT citus_pause_node_within_txn(1);",
		qr/cannot manage Citus node metadata when citus\.enable_distributed_engine is disabled/,
		'citus_pause_node_within_txn is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT create_distributed_function('noop_add(int)'::regprocedure, NULL::text, 'default');",
		qr/cannot create distributed functions when citus\.enable_distributed_engine is disabled/,
		'create_distributed_function is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT undistribute_table('dist_attempt'::regclass, false);",
		qr/cannot undistribute tables when citus\.enable_distributed_engine is disabled/,
		'undistribute_table is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT alter_distributed_table('dist_attempt'::regclass, 'id', NULL::integer, NULL::text, NULL::boolean);",
		qr/cannot alter distributed tables when citus\.enable_distributed_engine is disabled/,
		'alter_distributed_table is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT update_distributed_table_colocation('dist_attempt'::regclass, 'none');",
		qr/cannot alter distributed table colocation when citus\.enable_distributed_engine is disabled/,
		'update_distributed_table_colocation is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT replicate_reference_tables('auto'::citus.shard_transfer_mode);",
		qr/cannot replicate reference tables when citus\.enable_distributed_engine is disabled/,
		'replicate_reference_tables is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT citus_rebalance_start();",
		qr/cannot run shard rebalancing operations when citus\.enable_distributed_engine is disabled/,
		'citus_rebalance_start is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT * FROM get_rebalance_progress();",
		qr/cannot inspect rebalance progress when citus\.enable_distributed_engine is disabled/,
		'get_rebalance_progress is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT master_create_empty_shard('dist_attempt');",
		qr/cannot manage distributed shard placements when citus\.enable_distributed_engine is disabled/,
		'master_create_empty_shard is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT citus_schema_distribute('tenant_attempt'::regnamespace);",
		qr/cannot run schema-based sharding operations when citus\.enable_distributed_engine is disabled/,
		'citus_schema_distribute is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT citus_split_shard_by_split_points(1, ARRAY['10'], ARRAY[1], 'auto'::citus.shard_transfer_mode);",
		qr/cannot run shard split operations when citus\.enable_distributed_engine is disabled/,
		'citus_split_shard_by_split_points is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"CALL citus_cleanup_orphaned_resources();",
		qr/cannot run distributed orphan cleanup when citus\.enable_distributed_engine is disabled/,
		'citus_cleanup_orphaned_resources is rejected');

	fails_like_sql(
		$node,
		'columnar_db',
		"SELECT citus_unmark_object_distributed('pg_class'::regclass, 'reg_a'::regclass, 0, false);",
		qr/cannot manage distributed object metadata when citus\.enable_distributed_engine is disabled/,
		'citus_unmark_object_distributed is rejected');

	is($node->safe_psql('columnar_db',
						'SELECT count(*) FROM pg_catalog.pg_dist_partition;'),
	   '0',
	   'blocked distributed operations leave pg_dist_partition empty');
};

subtest 'incompatible databases are rejected after restart with engine off' => sub
{
	my $node = create_single_node(
		'engine_guard',
		"citus.enable_distributed_engine = on\n");

	$node->safe_psql('postgres', 'CREATE DATABASE guard_db;');
	$node->safe_psql('postgres', 'CREATE DATABASE plain_db;');

	$node->safe_psql('plain_db', 'CREATE EXTENSION citus;');
	$node->safe_psql('guard_db', q{
		CREATE EXTENSION citus;
		CREATE TABLE dist_guard(id int PRIMARY KEY, payload text);
		SELECT create_distributed_table('dist_guard', 'id');
	});

	is($node->safe_psql('guard_db',
						'SELECT count(*) FROM pg_catalog.pg_dist_partition;'),
	   '1',
	   'guard_db contains distributed metadata before restart');

	$node->stop('fast');
	$node->append_conf('postgresql.conf', "citus.enable_distributed_engine = off\n");
	$node->start();

	is($node->safe_psql('plain_db',
						"SELECT current_setting('citus.enable_distributed_engine');"),
	   'off',
	   'the restarted node is running with the engine disabled');

	is($node->safe_psql('plain_db', 'SELECT 1;'),
	   '1',
	   'a compatible database remains accessible after restart');

	fails_like_sql(
		$node,
		'guard_db',
		'SELECT 1;',
		qr/citus\.enable_distributed_engine cannot be disabled for database "guard_db"/,
		'planner path rejects an incompatible database');

	fails_like_sql(
		$node,
		'guard_db',
		'CREATE TABLE should_not_happen(a int);',
		qr/citus\.enable_distributed_engine cannot be disabled for database "guard_db"/,
		'utility path rejects an incompatible database');

	is($node->safe_psql('plain_db', 'SELECT 1;'),
	   '1',
	   'server stays usable after rejecting an incompatible database');
};

done_testing();

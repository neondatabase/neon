from fixtures.neon_fixtures import NeonEnvBuilder
from uuid import UUID
import tarfile
import os


def test_import(neon_env_builder,
                port_distributor,
                default_broker,
                mock_s3_server,
                test_output_dir,
                pg_bin):
    """Move a timeline to a new neon stack using pg_basebackup as interface."""
    node_name = "test_import"
    source_repo_dir = os.path.join(test_output_dir, "source_repo")
    destination_repo_dir = os.path.join(test_output_dir, "destination_repo")
    basebackup_dir = os.path.join(test_output_dir, "basebackup")
    basebackup_tar_path = os.path.join(test_output_dir, "basebackup.tar.gz")
    os.mkdir(basebackup_dir)

    # Create a repo, put some data in, take basebackup, and shut it down
    with NeonEnvBuilder(source_repo_dir, port_distributor, default_broker, mock_s3_server) as builder:
        env = builder.init_start()
        env.neon_cli.create_branch(node_name)
        pg = env.postgres.create_start(node_name)
        pg.safe_psql("create table t as select generate_series(1,300000)")
        assert pg.safe_psql('select count(*) from t') == [(300000, )]

        tenant = pg.safe_psql("show neon.tenant_id")[0][0]
        timeline = pg.safe_psql("show neon.timeline_id")[0][0]
        pg_bin.run(["pg_basebackup", "-d", pg.connstr(), "-D", basebackup_dir])

    # compress basebackup
    with tarfile.open(basebackup_tar_path, "w:gz") as tf:
        # TODO match iteration order to what pageserver would do
        tf.add(basebackup_dir)

    # Create a new repo, load the basebackup into it, and check that data is there
    with NeonEnvBuilder(destination_repo_dir, port_distributor, default_broker, mock_s3_server) as builder:
        env = builder.init_start()
        env.neon_cli.create_tenant(UUID(tenant))
        env.neon_cli.raw_cli([
            "timeline",
            "import",
            "--tenant-id", tenant,
            "--timeline-id", timeline,
            "--node-name", node_name,
            "--tarfile", basebackup_tar_path,
        ])
        pg = env.postgres.create_start(node_name, tenant_id=UUID(tenant))
        assert pg.safe_psql('select count(*) from t') == [(300000, )]

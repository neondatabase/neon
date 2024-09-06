import pytest
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.port_distributor import PortDistributor


# Test that neon cli is able to start and stop all processes with the user defaults.
# Repeats the example from README.md as close as it can
def test_neon_cli_basics(neon_env_builder: NeonEnvBuilder, port_distributor: PortDistributor):
    env = neon_env_builder.init_configs()
    # Skipping the init step that creates a local tenant in Pytest tests
    try:
        env.neon_cli.start()
        env.neon_cli.create_tenant(tenant_id=env.initial_tenant, set_default=True)

        main_branch_name = "main"
        pg_port = port_distributor.get_port()
        http_port = port_distributor.get_port()
        env.neon_cli.endpoint_create(
            main_branch_name, pg_port, http_port, endpoint_id="ep-basic-main"
        )
        env.neon_cli.endpoint_start("ep-basic-main")

        branch_name = "migration-check"
        env.neon_cli.create_branch(branch_name)
        pg_port = port_distributor.get_port()
        http_port = port_distributor.get_port()
        env.neon_cli.endpoint_create(
            branch_name, pg_port, http_port, endpoint_id=f"ep-{branch_name}"
        )
        env.neon_cli.endpoint_start(f"ep-{branch_name}")
    finally:
        env.neon_cli.stop()


def test_neon_two_primary_endpoints_fail(
    neon_env_builder: NeonEnvBuilder, port_distributor: PortDistributor
):
    """
    Two primary endpoints with same tenant and timeline will not run together
    """
    env = neon_env_builder.init_start()
    branch_name = "main"

    pg_port = port_distributor.get_port()
    http_port = port_distributor.get_port()
    env.neon_cli.endpoint_create(branch_name, pg_port, http_port, "ep1")

    pg_port = port_distributor.get_port()
    http_port = port_distributor.get_port()
    # ep1 is not running so create will succeed
    env.neon_cli.endpoint_create(branch_name, pg_port, http_port, "ep2")

    env.neon_cli.endpoint_start("ep1")

    expected_message = f'attempting to create a duplicate primary endpoint on tenant {env.initial_tenant}, timeline {env.initial_timeline}: endpoint "ep1" exists already. please don\'t do this, it is not supported.'
    with pytest.raises(RuntimeError):
        assert expected_message in env.neon_cli.endpoint_start("ep2").stderr

    env.neon_cli.endpoint_stop("ep1")
    # ep1 is stopped so create ep2 will succeed
    env.neon_cli.endpoint_start("ep2")
    # cleanup
    env.neon_cli.endpoint_stop("ep2")

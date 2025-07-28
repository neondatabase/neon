from __future__ import annotations

import json
import os
import shutil
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import TYPE_CHECKING

import requests
from fixtures.log_helper import log
from typing_extensions import override

if TYPE_CHECKING:
    from typing import Any

    from fixtures.common_types import TenantId, TimelineId
    from fixtures.neon_fixtures import NeonEnv
    from fixtures.port_distributor import PortDistributor


def launch_compute_ctl(
    env: NeonEnv,
    endpoint_name: str,
    external_http_port: int,
    internal_http_port: int,
    pg_port: int,
    control_plane_port: int,
) -> subprocess.Popen[str]:
    """
    Helper function to launch compute_ctl process with common configuration.
    Returns the Popen process object.
    """
    # Create endpoint directory structure following the standard pattern
    endpoint_path = env.repo_dir / "endpoints" / endpoint_name

    # Clean up any existing endpoint directory to avoid conflicts
    if endpoint_path.exists():
        shutil.rmtree(endpoint_path)

    endpoint_path.mkdir(mode=0o755, parents=True, exist_ok=True)

    # pgdata path - compute_ctl will create this directory during basebackup
    pgdata_path = endpoint_path / "pgdata"

    # Create log file in endpoint directory
    log_file = endpoint_path / "compute.log"
    log_handle = open(log_file, "w")

    # Start compute_ctl pointing to our control plane
    compute_ctl_path = env.neon_binpath / "compute_ctl"
    connstr = f"postgresql://cloud_admin@localhost:{pg_port}/postgres"

    # Find postgres binary path
    pg_bin_path = env.pg_distrib_dir / env.pg_version.v_prefixed / "bin" / "postgres"
    pg_lib_path = env.pg_distrib_dir / env.pg_version.v_prefixed / "lib"

    env_vars = {
        "INSTANCE_ID": "lakebase-instance-id",
        "LD_LIBRARY_PATH": str(pg_lib_path),  # Linux, etc.
        "DYLD_LIBRARY_PATH": str(pg_lib_path),  # macOS
    }

    cmd = [
        str(compute_ctl_path),
        "--external-http-port",
        str(external_http_port),
        "--internal-http-port",
        str(internal_http_port),
        "--pgdata",
        str(pgdata_path),
        "--connstr",
        connstr,
        "--pgbin",
        str(pg_bin_path),
        "--compute-id",
        endpoint_name,  # Use endpoint_name as compute-id
        "--control-plane-uri",
        f"http://127.0.0.1:{control_plane_port}",
        "--lakebase-mode",
        "true",
    ]

    print(f"Launching compute_ctl with command: {cmd}")

    # Start compute_ctl
    process = subprocess.Popen(
        cmd,
        env=env_vars,
        stdout=log_handle,
        stderr=subprocess.STDOUT,  # Combine stderr with stdout
        text=True,
    )

    return process


def wait_for_compute_status(
    compute_process: subprocess.Popen[str],
    http_port: int,
    expected_status: str,
    timeout_seconds: int = 10,
) -> None:
    """
    Wait for compute_ctl to reach the expected status.
    Raises an exception if timeout is reached or process exits unexpectedly.
    """
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        try:
            # Try to connect to the HTTP endpoint
            response = requests.get(f"http://localhost:{http_port}/status", timeout=0.5)
            if response.status_code == 200:
                status_json = response.json()
                # Check if it's in expected status
                if status_json.get("status") == expected_status:
                    return
        except (requests.ConnectionError, requests.Timeout):
            pass

        # Check if process has exited
        if compute_process.poll() is not None:
            raise Exception(
                f"compute_ctl exited unexpectedly with code {compute_process.returncode}."
            )

        time.sleep(0.5)

    # Timeout reached
    compute_process.terminate()
    raise Exception(
        f"compute_ctl failed to reach {expected_status} status within {timeout_seconds} seconds."
    )


class EmptySpecHandler(BaseHTTPRequestHandler):
    """HTTP handler that returns an Empty compute spec response"""

    def do_GET(self):
        if self.path.startswith("/compute/api/v2/computes/") and self.path.endswith("/spec"):
            # Return empty status which will put compute in Empty state
            response: dict[str, Any] = {
                "status": "empty",
                "spec": None,
                "compute_ctl_config": {"jwks": {"keys": []}},
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_error(404)

    @override
    def log_message(self, format: str, *args: Any):
        # Suppress request logging
        pass


def test_compute_terminate_empty(neon_simple_env: NeonEnv, port_distributor: PortDistributor):
    """
    Test that terminating a compute in Empty status works correctly.

    This tests the bug fix where terminating an Empty compute would hang
    waiting for a non-existent postgres process to terminate.
    """
    env = neon_simple_env

    # Get ports for our test
    control_plane_port = port_distributor.get_port()
    external_http_port = port_distributor.get_port()
    internal_http_port = port_distributor.get_port()
    pg_port = port_distributor.get_port()

    # Start a simple HTTP server that will serve the Empty spec
    server = HTTPServer(("127.0.0.1", control_plane_port), EmptySpecHandler)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    compute_process = None
    try:
        # Start compute_ctl with ephemeral tenant ID
        compute_process = launch_compute_ctl(
            env,
            "test-empty-compute",
            external_http_port,
            internal_http_port,
            pg_port,
            control_plane_port,
        )

        # Wait for compute_ctl to start and report "empty" status
        wait_for_compute_status(compute_process, external_http_port, "empty")

        # Now send terminate request
        response = requests.post(f"http://localhost:{external_http_port}/terminate")

        # Verify that the termination request sends back a 200 OK response and is not abruptly terminated.
        assert response.status_code == 200, (
            f"Expected 200 OK, got {response.status_code}: {response.text}"
        )

        # Wait for compute_ctl to exit
        exit_code = compute_process.wait(timeout=10)
        assert exit_code == 0, f"compute_ctl exited with non-zero code: {exit_code}"

    finally:
        # Clean up
        server.shutdown()
        if compute_process and compute_process.poll() is None:
            compute_process.terminate()
            compute_process.wait()


class SwitchableConfigHandler(BaseHTTPRequestHandler):
    """HTTP handler that can switch between normal compute configs and compute configs without specs"""

    return_empty_spec: bool = False
    tenant_id: TenantId | None = None
    timeline_id: TimelineId | None = None
    pageserver_port: int | None = None
    safekeeper_connstrs: list[str] | None = None

    def do_GET(self):
        if self.path.startswith("/compute/api/v2/computes/") and self.path.endswith("/spec"):
            if self.return_empty_spec:
                # Return empty status
                response: dict[str, object | None] = {
                    "status": "empty",
                    "spec": None,
                    "compute_ctl_config": {
                        "jwks": {"keys": []},
                    },
                }
            else:
                # Return normal attached spec
                response = {
                    "status": "attached",
                    "spec": {
                        "format_version": 1.0,
                        "cluster": {
                            "roles": [],
                            "databases": [],
                            "postgresql_conf": "shared_preload_libraries='neon'",
                        },
                        "tenant_id": str(self.tenant_id) if self.tenant_id else "",
                        "timeline_id": str(self.timeline_id) if self.timeline_id else "",
                        "pageserver_connstring": f"postgres://no_user@localhost:{self.pageserver_port}"
                        if self.pageserver_port
                        else "",
                        "safekeeper_connstrings": self.safekeeper_connstrs or [],
                        "mode": "Primary",
                        "skip_pg_catalog_updates": True,
                        "reconfigure_concurrency": 1,
                        "suspend_timeout_seconds": -1,
                    },
                    "compute_ctl_config": {
                        "jwks": {"keys": []},
                    },
                }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_error(404)

    @override
    def log_message(self, format: str, *args: Any):
        # Suppress request logging
        pass


def test_compute_empty_spec_during_refresh_configuration(
    neon_simple_env: NeonEnv, port_distributor: PortDistributor
):
    """
    Test that compute exits when it receives an empty spec during refresh configuration state.

    This test:
    1. Start compute with a normal spec
    2. Change the spec handler to return empty spec
    3. Trigger some condition to force compute to refresh configuration
    4. Verify that compute_ctl exits
    """
    env = neon_simple_env

    # Get ports for our test
    control_plane_port = port_distributor.get_port()
    external_http_port = port_distributor.get_port()
    internal_http_port = port_distributor.get_port()
    pg_port = port_distributor.get_port()

    # Set up handler class variables
    SwitchableConfigHandler.tenant_id = env.initial_tenant
    SwitchableConfigHandler.timeline_id = env.initial_timeline
    SwitchableConfigHandler.pageserver_port = env.pageserver.service_port.pg
    # Convert comma-separated string to list
    safekeeper_connstrs = env.get_safekeeper_connstrs()
    if safekeeper_connstrs:
        SwitchableConfigHandler.safekeeper_connstrs = safekeeper_connstrs.split(",")
    else:
        SwitchableConfigHandler.safekeeper_connstrs = []
    SwitchableConfigHandler.return_empty_spec = False  # Start with normal spec

    # Start HTTP server with switchable spec handler
    server = HTTPServer(("127.0.0.1", control_plane_port), SwitchableConfigHandler)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    compute_process = None
    try:
        # Start compute_ctl with tenant and timeline IDs
        # Use a unique endpoint name to avoid conflicts
        endpoint_name = f"test-refresh-compute-{os.getpid()}"
        compute_process = launch_compute_ctl(
            env,
            endpoint_name,
            external_http_port,
            internal_http_port,
            pg_port,
            control_plane_port,
        )

        # Wait for compute_ctl to start and report "running" status
        wait_for_compute_status(compute_process, external_http_port, "running", timeout_seconds=30)

        log.info("Compute is running. Now returning empty spec and trigger configuration refresh.")

        # Switch spec fetch handler to return empty spec
        SwitchableConfigHandler.return_empty_spec = True

        # Trigger a configuration refresh
        try:
            requests.post(f"http://localhost:{internal_http_port}/refresh_configuration")
        except requests.RequestException as e:
            log.info(f"Call to /refresh_configuration failed: {e}")
            log.info(
                "Ignoring the error, assuming that compute_ctl is already refreshing or has exited"
            )

        # Wait for compute_ctl to exit (it should exit when it gets an empty spec during refresh)
        exit_start_time = time.time()
        while time.time() - exit_start_time < 30:
            if compute_process.poll() is not None:
                # Process exited
                break
            time.sleep(0.5)

        # Verify that compute_ctl exited
        exit_code = compute_process.poll()
        if exit_code is None:
            compute_process.terminate()
            raise Exception("compute_ctl did not exit after receiving empty spec.")

        # The exit code might not be 0 in this case since it's an unexpected termination
        # but we mainly care that it did exit
        assert exit_code is not None, "compute_ctl should have exited"

    finally:
        # Clean up
        server.shutdown()
        if compute_process and compute_process.poll() is None:
            compute_process.terminate()
            compute_process.wait()

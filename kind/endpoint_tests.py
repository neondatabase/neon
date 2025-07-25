import json
import logging
import re
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import backoff
import jwt
import psycopg2
import requests
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from kind_test_env import (
    HADRON_COMPUTE_IMAGE_NAME,
    HADRON_MT_IMAGE_NAME,
    HadronEndpoint,
    KindTestEnvironment,
    NodeType,
    check_prerequisites,
    read_table_data,
    unique_node_id,
    write_table_data,
)

logging.getLogger("backoff").addHandler(logging.StreamHandler())


def read_private_key_and_public_key(
    privkey_filename: str, certificate_path: str
) -> tuple[str, str]:
    # Get certificate serial number
    with open(certificate_path, "rb") as pem_file:
        content = pem_file.read()
    certificate = x509.load_pem_x509_certificate(content, default_backend())
    certificate_thumbprint = certificate.fingerprint(hashes.SHA256()).hex()

    # Get private key content string.
    private_key_content = Path(privkey_filename).read_text()
    return (private_key_content, certificate_thumbprint)


def generate_token(
    testuser: str,
    endpoint_id: str,
    database: str,
    cert_directory: str,
    payload: dict[str, Any] | None = None,
) -> str:
    """
    Generate a JWT token for a testuser using the private key specified in the environment.

    :param testuser: user name to generate the token for.
    :param endpoint_id: hadron endpoint id.
    :param database: database to connect to.
    :param cert_directory: directory containing the private key and public key for generating the token.
    :param payload: additional payload. It will be merged with the default payload.
    :return:
    """
    (private_key, certificate_thumbprint) = read_private_key_and_public_key(
        f"{cert_directory}/privkey1.pem", f"{cert_directory}/pubkey1.pem"
    )
    expiration = time.time() + (10 * 60)  # Expiration time is 10 minutes.

    payload = {
        "sub": testuser,
        "iss": "brickstore.databricks.com",
        "exp": int(expiration),
        "endpointId": endpoint_id,
        "database": database,
    } | (payload or {})

    token = jwt.encode(
        payload, private_key, algorithm="RS256", headers={"kid": certificate_thumbprint}
    )
    return token


def check_token_login(
    endpoint: HadronEndpoint, endpoint_id: str, database: str, cert_directory: str
) -> None:
    """
    Check that we can login to the endpoint using a JWT token.
    """
    # Create test_token_user
    testuser = "test_token_user"
    with endpoint.cursor() as cur:
        cur.execute(f"CREATE ROLE {testuser} LOGIN PASSWORD NULL;")

    # Login with the token
    token = generate_token(testuser, endpoint_id, database, cert_directory)
    with endpoint.cursor(user=testuser, password=token) as c:
        c.execute("select current_user")
        result = c.fetchone()
        assert result == (testuser,)


def check_databricks_roles(endpoint: HadronEndpoint) -> None:
    """
    Check that the expected Databricks roles are present in the endpoint.
    """
    expected_roles = ["databricks_monitor", "databricks_control_plane", "databricks_gateway"]
    with endpoint.cursor() as cur:
        for role in expected_roles:
            cur.execute(f"SELECT 1 FROM pg_roles WHERE rolname = '{role}';")
            result = cur.fetchone()
            assert result == (1,)


def test_create_endpoint_and_connect() -> None:
    """
    Tests that we can create an endpoint on a Hadron deployment and connect to it/run simple queries.
    """

    with KindTestEnvironment() as env:
        env.load_image(HADRON_MT_IMAGE_NAME)
        env.load_image(HADRON_COMPUTE_IMAGE_NAME)

        # Setup the Hadron deployment in the brand new KIND cluster.
        # We have 2 PS, 3 SK, and mocked S3 storage in the test.
        env.start_hcc_with_configmaps(
            "configmaps/configmap_2ps_3sk.yaml", "configmaps/safe_configmap_1.yaml"
        )

        ps_id_0 = unique_node_id(0, 0)
        ps_id_1 = unique_node_id(0, 1)

        # Wait for all the Hadron storage components to come up.
        env.wait_for_app_ready("storage-controller", namespace="hadron")
        env.wait_for_statefulset_replicas("safe-keeper-0", replicas=3, namespace="hadron")
        env.wait_for_statefulset_replicas("page-server-0", replicas=2, namespace="hadron")
        env.wait_for_node_registration(NodeType.PAGE_SERVER, {ps_id_0, ps_id_1})
        env.wait_for_node_registration(
            NodeType.SAFE_KEEPER, {unique_node_id(0, 0), unique_node_id(0, 1), unique_node_id(0, 2)}
        )

        @backoff.on_exception(backoff.expo, psycopg2.Error, max_tries=10)
        def check_superuser_and_basic_data_operation(endpoint):
            with endpoint.cursor() as cur:
                # Check whether the current user is the super user.
                cur.execute("SELECT usesuper FROM pg_user WHERE usename = CURRENT_USER")
                is_superuser = cur.fetchone()[0]

                # Create a simple table and insert some data.
                cur.execute("DROP TABLE IF EXISTS t")
                cur.execute("CREATE TABLE t (x int)")
                cur.execute("INSERT INTO t VALUES (1), (2), (3)")
                cur.execute("SELECT * FROM t")
                rows = cur.fetchall()

                # Test that the HCC-created default user was indeed the super user
                # and that we can retrieve the same data we inserted from the table.
                assert is_superuser is True, "Current user is not a superuser"
                assert rows == [(1,), (2,), (3,)], f"Data retrieval mismatch: {rows}"

                # Verify that the server has ssl turn on .
                cur.execute("SHOW SSL;")
                result = cur.fetchone()
                assert result == ("on",)

                # Verify that the connection is using SSL.
                cur.execute("SELECT SSL FROM pg_stat_ssl WHERE pid = pg_backend_pid();")
                result = cur.fetchone()
                assert result == (True,)

        @backoff.on_exception(backoff.expo, psycopg2.Error, max_tries=10)
        def check_databricks_system_tables(endpoint):
            with endpoint.cursor(dbname="databricks_system") as cur:
                # Verify that the LFC is working by querying the LFC stats table.
                # If the LFC is not running, the table will contain a single row with all NULL values.
                cur.execute(
                    "SELECT 1 FROM neon.NEON_STAT_FILE_CACHE WHERE file_cache_misses IS NOT NULL;"
                )
                lfcStatsRows = cur.fetchall()

                assert len(lfcStatsRows) == 1, "LFC stats table is empty"

        # Check that the system-level GUCs are set to the expected values. These should be set before the endpoint
        # starts accepting connections.
        def check_guc_values(endpoint):
            with endpoint.cursor() as cur:
                cur.execute("SHOW databricks.workspace_url;")
                res = cur.fetchone()[0]
                print(f"atabricks.workspace_url: {res}")
                assert res == urlparse(test_workspace_url).hostname, (
                    "Failed to get the correct databricks.workspace_url GUC value"
                )
                cur.execute("SHOW databricks.enable_databricks_identity_login;")
                res = cur.fetchone()[0]
                print(f"databricks.enable_databricks_identity_login: {res}")
                assert res == "on", (
                    "Failed to get the correct databricks.enable_databricks_identity_login GUC value"
                )
                cur.execute("SHOW databricks.enable_sql_restrictions;")
                res = cur.fetchone()[0]
                print(f"databricks.enable_sql_restrictions: {res}")
                assert res == "on", (
                    "Failed to get the correct databricks.enable_sql_restrictions GUC value"
                )
                cur.execute("SHOW databricks.disable_PAT_login;")
                res = cur.fetchone()[0]
                print(f"databricks.disable_PAT_login: {res}")
                assert res == "on", (
                    "Failed to get the correct databricks.disable_PAT_login GUC value"
                )

        def check_cert_auth_user(endpoint):
            expected_user = "databricks_control_plane"
            with endpoint.cursor(
                user=expected_user,
                sslcert=f"{env.tempDir.name}/pubkey1.pem",
                sslkey=f"{env.tempDir.name}/privkey1.pem",
                sslmode="require",
            ) as cur:
                cur.execute("select current_user;")
                current_user = cur.fetchone()[0]
                assert current_user == expected_user, f"{current_user} is not {expected_user}"

        # Query the "neon.pageserver_connstring" Postgres GUC to see which pageserver the compute node is currently connected to.
        def check_current_ps_id(endpoint: HadronEndpoint) -> int:
            with endpoint.cursor() as cur:
                cur.execute("SHOW neon.pageserver_connstring;")
                res = cur.fetchone()
                assert res is not None, "Failed to get the current pageserver connection URL"
                connection_url = res[0]
                print(f"Current pageserver connection URL is {connection_url}")
                host = urlparse(connection_url).hostname
                # In this test, the hostname is in the form of "page-server-{pool}-{ordinal}.page-server.hadron.svc.cluster.local"
                # We extract the "page-server-{pool}-{ordinal}" part and convert the two numbers into the pageserver ID.
                pool_id, ordinal_id = host.split(".")[0].split("-")[-2:]
                return unique_node_id(int(pool_id), int(ordinal_id))

        def verify_compute_pod_metadata(pod_name: str, pod_namespace: str, endpoint_id: uuid.UUID):
            # Check that dblet-required labels and annotations are present on the compute pod.
            # See go/dblet-labels
            compute_pod = env.kubectl_get_pod(namespace=pod_namespace, pod_name=pod_name)
            assert (
                compute_pod["metadata"]["annotations"]["databricks.com/workspace-url"]
                == urlparse(test_workspace_url).hostname
            )
            assert compute_pod["metadata"]["labels"]["orgId"] == test_workspace_id
            assert compute_pod["metadata"]["labels"]["dblet.dev/appid"] == f"{endpoint_id}-0"

        def check_pg_log_redaction(pod_name: str, container_name: str, pod_namespace: str):
            """
            Various checks to ensure that the PG log redactor is working as expected via comparing
            PG log vs. redacted log.

            Checks between original from PG and redacted log:
              - log folders exist
              - there's at least 1 log file
              - the number of files match
              - number of log entries is close
              - the last redacted log entry is in the last few PG log entries, ignoring the
                redacted message field
            """
            # a little higher than redactor flush entries
            lag_tolerance_items = 22
            MESSAGE_FIELD = "message"
            LOG_DAEMON_EXPECTED_REGEX = r"hadron-compute-redacted-[a-zA-Z]{3}[0-9]{4}\.json"
            redactor_file_count_catchup_timeout_seconds = 60

            def kex(command: list[str]) -> str:
                # mypy can't tell kubectl_exec returns str
                result: str = env.kubectl_exec(pod_namespace, pod_name, container_name, command)
                return result

            log_folders = kex(["ls", "/databricks/logs/"]).split()
            assert "brickstore" in log_folders, "PG log folder not found"
            assert "brickstore-redacted" in log_folders, "Redacted log folder not found"

            @backoff.on_exception(
                backoff.expo, AssertionError, max_time=redactor_file_count_catchup_timeout_seconds
            )
            def get_caught_up_files() -> tuple[list[str], list[str]]:
                """
                Get pg (input) and redacted (output) log files after verifying the files exist and the counts match.

                @return: tuple of:
                    - list of pg log file names
                    - list of redacted log file names
                """
                pg_log_files = kex(["ls", "-t", "/databricks/logs/brickstore/"]).split()
                redacted_log_files = kex(
                    ["ls", "-t", "/databricks/logs/brickstore-redacted/"]
                ).split()
                pg_log_files = [file for file in pg_log_files if ".json" in file]
                print("Compute log files:", pg_log_files, redacted_log_files)
                assert len(pg_log_files) > 0, "PG didn't produce any JSON log files"
                assert len(redacted_log_files) > 0, "Redactor didn't produce any log files"
                assert len(pg_log_files) == len(redacted_log_files), (
                    "Redactor didn't process each log file exactly once"
                )
                for file in redacted_log_files:
                    assert re.match(LOG_DAEMON_EXPECTED_REGEX, file), (
                        f"Unexpected redacted log file name: {file}"
                    )
                return pg_log_files, redacted_log_files

            # wait for pg_log_redactor to catch up, by file count
            pg_log_files, redacted_log_files = get_caught_up_files()

            # Rest will examine latest files closer
            last_pg_log_file = pg_log_files[0]
            last_redacted_log_file = redacted_log_files[0]

            pg_log_entries_num = int(
                kex(["wc", "-l", f"/databricks/logs/brickstore/{last_pg_log_file}"]).split()[0]
            )
            redacted_log_entries_num = int(
                kex(
                    ["wc", "-l", f"/databricks/logs/brickstore-redacted/{last_redacted_log_file}"]
                ).split()[0]
            )
            assert redacted_log_entries_num <= pg_log_entries_num, (
                "Redactor emitted non-PG log messages, either through bug or own error msg."
            )
            assert redacted_log_entries_num - pg_log_entries_num < lag_tolerance_items, (
                "Redactor lagged behind, more than OS buffering should allow for."
            )

            # Order to decrease chance of lag flakiness
            pg_log_tail = kex(
                [
                    "tail",
                    "-n",
                    str(lag_tolerance_items),
                    f"/databricks/logs/brickstore/{last_pg_log_file}",
                ]
            )
            redacted_log_tail_item = kex(
                [
                    "tail",
                    "-n",
                    "1",
                    f"/databricks/logs/brickstore-redacted/{last_redacted_log_file}",
                ]
            )

            redacted_log_tail_json = json.loads(redacted_log_tail_item)
            if MESSAGE_FIELD in redacted_log_tail_json:
                del redacted_log_tail_json[MESSAGE_FIELD]
            found_in_pg_log = False
            for pg_log_item in pg_log_tail.split("\n"):
                pg_log_json = json.loads(pg_log_item)
                if MESSAGE_FIELD in pg_log_json:
                    del pg_log_json[MESSAGE_FIELD]
                if redacted_log_tail_json == pg_log_json:
                    found_in_pg_log = True
                    break
            # Note: lag is possible because tail call is not synced w/ lag check and there's no simple way to
            assert found_in_pg_log, (
                "Last log seen in redactor is not a recent log from PG, through lag bug or own error msg"
            )

        # Create an endpoint with random IDs.
        test_metastore_id = uuid.uuid4()
        test_endpoint_id = uuid.uuid4()
        test_workspace_id = "987654321"
        test_workspace_url = "https://test-workspace-url/"
        compute_namespace = ""
        compute_name = ""
        with env.hcc_create_endpoint(
            test_metastore_id, test_endpoint_id, test_workspace_id, test_workspace_url
        ) as endpoint:
            check_superuser_and_basic_data_operation(endpoint)
            check_databricks_system_tables(endpoint)
            check_databricks_roles(endpoint)
            check_guc_values(endpoint)
            check_cert_auth_user(endpoint)
            check_token_login(endpoint, str(test_endpoint_id), "postgres", env.tempDir.name)

            write_table_data(endpoint, "my_table", ["a", "b", "c"])
            assert read_table_data(endpoint, "my_table") == ["a", "b", "c"]

            compute_name = endpoint.name
            compute_namespace = endpoint.namespace

            hadron_compute_pods = env.kubectl_pods(namespace=compute_namespace)
            hadron_compute_pod_name = hadron_compute_pods[0]
            verify_compute_pod_metadata(
                hadron_compute_pod_name, compute_namespace, test_endpoint_id
            )

            # Check in compute log that we have initialized the Databricks extension.
            logs = env.kubectl_logs(namespace=compute_namespace, pod_name=hadron_compute_pod_name)
            assert "Databricks extension initialized" in logs, "Endpoint creation not logged"

            # Check that metrics are exported
            r = requests.get(endpoint.metrics_url)
            assert r.status_code == 200
            assert "pg_static" in r.text
            # Check for this particular metric to make sure prometheus exporter has the permission to
            # execute wal-related functions such as `pg_current_wal_lsn`, `pg_wal_lsn_diff`, etc.
            assert "pg_replication_slots_pg_wal_lsn_diff" in r.text
            # Check for these metrics from function or view in neon schema in databricks_system database
            # to ensure extra grants were successful
            assert "pg_backpressure_throttling_time" in r.text
            assert "pg_lfc_hits" in r.text
            assert "pg_lfc_working_set_size" in r.text
            assert "pg_cluster_size_bytes" in r.text
            assert "pg_snapshot_files_count" in r.text
            assert re.search(r"pg_writable_bool{.*} 1", r.text)
            assert "pg_database_size_bytes" not in r.text
            # Check for this label key to ensure that the metrics are being labeled correctly for PuPr model
            assert "pg_instance_id=" in r.text
            assert "pg_metrics_sql_index_corruption_count" in r.text
            assert "pg_metrics_num_active_safekeepers" in r.text
            assert "pg_metrics_num_configured_safekeepers" in r.text
            assert "pg_metrics_max_active_safekeeper_commit_lag" in r.text

            check_pg_log_redaction(hadron_compute_pod_name, endpoint.container, compute_namespace)

            # Smoke test tenant migration
            curr_ps_id = check_current_ps_id(endpoint)
            new_ps_id = ps_id_0 if curr_ps_id == ps_id_1 else ps_id_1
            env.hcc_migrate_endpoint(test_endpoint_id, new_ps_id)
            assert check_current_ps_id(endpoint) == new_ps_id
            # Check that data operation still works after migration
            check_superuser_and_basic_data_operation(endpoint)
            # Check that the data we wrote before migration stays untouched
            assert read_table_data(endpoint, "my_table") == ["a", "b", "c"]

        # Restart the compute endpoint to clear any local caches to be extra sure that tenant migration indeed
        # does not lose data
        with env.restart_endpoint(compute_name, compute_namespace) as endpoint:
            # Check that data persists after the compute node restarts.
            assert read_table_data(endpoint, "my_table") == ["a", "b", "c"]
            # Check that data operations can resume after the restart
            check_superuser_and_basic_data_operation(endpoint)

            # PG compute reconciliation verification test. We intetionally run this test after the tenant migration
            # restart test so that the tenant migration test can observe the first, "untainted" restart.
            #
            # Update the cluster-config map's default PgParams and ensure that the compute instance is reconciled properly.
            # In this case, test updating the compute_http_port as a trivial example.
            current_configmap = env.get_configmap_json("cluster-config", "hadron")
            config_map_key = "config.json"
            config_json = json.loads(current_configmap["data"][config_map_key])

            if "pg_params" not in config_json:
                config_json["pg_params"] = {}

            test_http_port = 3456

            config_json["pg_params"]["compute_http_port"] = test_http_port

            patch_json = {"data": {"config.json": json.dumps(config_json)}}

            env.kubectl_patch(
                resource="configmap",
                name="cluster-config",
                namespace="hadron",
                json_patch=json.dumps(patch_json),
            )

            # Ensure that the deployment is updated by the HCC accordingly within 2 mintues.
            # Note that waiting this long makes sense since files mounted via a config maps
            # are not updated immediately, and instead updated every kubelet sync period (typically 1m)
            # on top of the kubelets configmap cahce TTL (which is also typically 1m).
            timeout = 120
            start_time = time.time()
            while True:
                deployment = env.get_deployment_json(compute_name, compute_namespace)
                port = deployment["spec"]["template"]["spec"]["containers"][0]["ports"][1]
                if port["containerPort"] == test_http_port:
                    print("Compute succesfully updated")
                    break
                else:
                    print(
                        f"Current PG HTTP port spec: {port}, Expected container port: {test_http_port}"
                    )

                if time.time() - start_time >= timeout:
                    raise Exception(f"Compute deployment did not update within {timeout} seconds")

                time.sleep(5)

            # Wait for the updated endpoint to become ready again after a rollout.
            env.wait_for_app_ready(compute_name, namespace="hadron-compute", timeout=60)

            # Verify that the updated compute pod has the correct workspace annotations/labels
            # as persisted in the metadata database.
            hadron_compute_pods = env.kubectl_pods(namespace=compute_namespace)
            hadron_compute_pod_name = hadron_compute_pods[0]
            verify_compute_pod_metadata(
                hadron_compute_pod_name, compute_namespace, test_endpoint_id
            )

        # Delete the endpoint.
        env.hcc_delete_endpoint(test_endpoint_id)
        # Ensure that k8s resources of the compute endpoint are deleted.
        env.wait_for_compute_resource_deletion(compute_name, compute_namespace)


if __name__ == "__main__":
    check_prerequisites()
    test_create_endpoint_and_connect()

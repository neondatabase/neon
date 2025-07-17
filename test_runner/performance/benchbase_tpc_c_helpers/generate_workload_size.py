import argparse
import math
import os
import sys
from pathlib import Path

CONFIGS_DIR = Path("../configs")
SCRIPTS_DIR = Path("../scripts")

# Constants
## TODO increase times after testing
WARMUP_TIME_SECONDS = 120  # 20 minutes
BENCHMARK_TIME_SECONDS = 360  # 1 hour
RAMP_STEP_TIME_SECONDS = 300  # 5 minutes
BASE_TERMINALS = 130
TERMINALS_PER_WAREHOUSE = 0.2
OPTIMAL_RATE_FACTOR = 0.7  # 70% of max rate
BATCH_SIZE = 1000
LOADER_THREADS = 4
TRANSACTION_WEIGHTS = "45,43,4,4,4"  # NewOrder, Payment, OrderStatus, Delivery, StockLevel
# Ramp-up rate multipliers
RAMP_RATE_FACTORS = [0.4, 0.6, 0.7, 0.9, 1.0, 1.1, 2.0]

# Templates for XML configs
WARMUP_XML = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://{hostname}/neondb?sslmode=require&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true</url>
    <username>neondb_owner</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_READ_COMMITTED</isolation>
    <batchsize>{batch_size}</batchsize>
    <scalefactor>{warehouses}</scalefactor>
    <loaderThreads>0</loaderThreads>
    <terminals>{terminals}</terminals>
    <works>
        <work>
            <time>{warmup_time}</time>
            <weights>{transaction_weights}</weights>
            <rate>unlimited</rate>
            <arrival>POISSON</arrival>
            <distribution>ZIPFIAN</distribution>
        </work>
    </works>
    <transactiontypes>
        <transactiontype><name>NewOrder</name></transactiontype>
        <transactiontype><name>Payment</name></transactiontype>
        <transactiontype><name>OrderStatus</name></transactiontype>
        <transactiontype><name>Delivery</name></transactiontype>
        <transactiontype><name>StockLevel</name></transactiontype>
    </transactiontypes>
</parameters>
"""

MAX_RATE_XML = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://{hostname}/neondb?sslmode=require&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true</url>
    <username>neondb_owner</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_READ_COMMITTED</isolation>
    <batchsize>{batch_size}</batchsize>
    <scalefactor>{warehouses}</scalefactor>
    <loaderThreads>0</loaderThreads>
    <terminals>{terminals}</terminals>
    <works>
        <work>
            <time>{benchmark_time}</time>
            <weights>{transaction_weights}</weights>
            <rate>unlimited</rate>
            <arrival>POISSON</arrival>
            <distribution>ZIPFIAN</distribution>
        </work>
    </works>
    <transactiontypes>
        <transactiontype><name>NewOrder</name></transactiontype>
        <transactiontype><name>Payment</name></transactiontype>
        <transactiontype><name>OrderStatus</name></transactiontype>
        <transactiontype><name>Delivery</name></transactiontype>
        <transactiontype><name>StockLevel</name></transactiontype>
    </transactiontypes>
</parameters>
"""

OPT_RATE_XML = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://{hostname}/neondb?sslmode=require&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true</url>
    <username>neondb_owner</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_READ_COMMITTED</isolation>
    <batchsize>{batch_size}</batchsize>
    <scalefactor>{warehouses}</scalefactor>
    <loaderThreads>0</loaderThreads>
    <terminals>{terminals}</terminals>
    <works>
        <work>
            <time>{benchmark_time}</time>
            <rate>{opt_rate}</rate>
            <weights>{transaction_weights}</weights>
            <arrival>POISSON</arrival>
            <distribution>ZIPFIAN</distribution>
        </work>
    </works>
    <transactiontypes>
        <transactiontype><name>NewOrder</name></transactiontype>
        <transactiontype><name>Payment</name></transactiontype>
        <transactiontype><name>OrderStatus</name></transactiontype>
        <transactiontype><name>Delivery</name></transactiontype>
        <transactiontype><name>StockLevel</name></transactiontype>
    </transactiontypes>
</parameters>
"""

RAMP_UP_XML = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://{hostname}/neondb?sslmode=require&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true</url>
    <username>neondb_owner</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_READ_COMMITTED</isolation>
    <batchsize>{batch_size}</batchsize>
    <scalefactor>{warehouses}</scalefactor>
    <loaderThreads>0</loaderThreads>
    <terminals>{terminals}</terminals>
    <works>
{works}
    </works>
    <transactiontypes>
        <transactiontype><name>NewOrder</name></transactiontype>
        <transactiontype><name>Payment</name></transactiontype>
        <transactiontype><name>OrderStatus</name></transactiontype>
        <transactiontype><name>Delivery</name></transactiontype>
        <transactiontype><name>StockLevel</name></transactiontype>
    </transactiontypes>
</parameters>
"""

WORK_TEMPLATE = f"""        <work>\n            <time>{RAMP_STEP_TIME_SECONDS}</time>\n            <rate>{{rate}}</rate>\n            <weights>{TRANSACTION_WEIGHTS}</weights>\n            <arrival>POISSON</arrival>\n            <distribution>ZIPFIAN</distribution>\n        </work>\n"""

# Templates for shell scripts
EXECUTE_SCRIPT = """# Create results directories
mkdir -p results_warmup
mkdir -p results_{suffix}
chmod 777 results_warmup results_{suffix}

# Run warmup phase
docker run --network=host --rm \
  -v $(pwd)/configs:/configs \
  -v $(pwd)/results_warmup:/results \
    {docker_image}\
  -b tpcc \
  -c /configs/execute_{warehouses}_warehouses_warmup.xml \
  -d /results \
  --create=false --load=false --execute=true

# Run benchmark phase
docker run --network=host --rm \
  -v $(pwd)/configs:/configs \
  -v $(pwd)/results_{suffix}:/results \
    {docker_image}\
  -b tpcc \
  -c /configs/execute_{warehouses}_warehouses_{suffix}.xml \
  -d /results \
  --create=false --load=false --execute=true\n"""

LOAD_XML = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://{hostname}/neondb?sslmode=require&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true</url>
    <username>neondb_owner</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_READ_COMMITTED</isolation>
    <batchsize>{batch_size}</batchsize>
    <scalefactor>{warehouses}</scalefactor>
    <loaderThreads>{loader_threads}</loaderThreads>
</parameters>
"""

LOAD_SCRIPT = """# Create results directory for loading
mkdir -p results_load
chmod 777 results_load

docker run --network=host --rm \
  -v $(pwd)/configs:/configs \
  -v $(pwd)/results_load:/results \
    {docker_image}\
  -b tpcc \
  -c /configs/load_{warehouses}_warehouses.xml \
  -d /results \
  --create=true --load=true --execute=false\n"""


def write_file(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, "w") as f:
            f.write(content)
    except OSError as e:
        print(f"Error writing {path}: {e}")
        sys.exit(1)
    # If it's a shell script, set executable permission
    if str(path).endswith(".sh"):
        os.chmod(path, 0o755)


def get_docker_arch_tag(runner_arch):
    """Map GitHub Actions runner.arch to Docker image architecture tag."""
    arch_mapping = {"X64": "amd64", "ARM64": "arm64"}
    return arch_mapping.get(runner_arch, "amd64")  # Default to amd64


def main():
    parser = argparse.ArgumentParser(description="Generate BenchBase workload configs and scripts.")
    parser.add_argument("--warehouses", type=int, required=True, help="Number of warehouses")
    parser.add_argument("--max-rate", type=int, required=True, help="Max rate (TPS)")
    parser.add_argument("--hostname", type=str, required=True, help="Database hostname")
    parser.add_argument("--password", type=str, required=True, help="Database password")
    parser.add_argument(
        "--runner-arch", type=str, required=True, help="GitHub Actions runner architecture"
    )
    args = parser.parse_args()

    warehouses = args.warehouses
    max_rate = args.max_rate
    hostname = args.hostname
    password = args.password
    runner_arch = args.runner_arch

    # Get the appropriate Docker architecture tag
    docker_arch = get_docker_arch_tag(runner_arch)
    docker_image = f"ghcr.io/neondatabase-labs/benchbase-postgres:latest-{docker_arch}"

    opt_rate = math.ceil(max_rate * OPTIMAL_RATE_FACTOR)
    # Calculate terminals as next rounded integer of 40% of warehouses
    terminals = math.ceil(BASE_TERMINALS + warehouses * TERMINALS_PER_WAREHOUSE)
    ramp_rates = [math.ceil(max_rate * factor) for factor in RAMP_RATE_FACTORS]

    # Write configs
    write_file(
        CONFIGS_DIR / f"execute_{warehouses}_warehouses_warmup.xml",
        WARMUP_XML.format(
            warehouses=warehouses,
            hostname=hostname,
            password=password,
            terminals=terminals,
            batch_size=BATCH_SIZE,
            warmup_time=WARMUP_TIME_SECONDS,
            transaction_weights=TRANSACTION_WEIGHTS,
        ),
    )
    write_file(
        CONFIGS_DIR / f"execute_{warehouses}_warehouses_max_rate.xml",
        MAX_RATE_XML.format(
            warehouses=warehouses,
            hostname=hostname,
            password=password,
            terminals=terminals,
            batch_size=BATCH_SIZE,
            benchmark_time=BENCHMARK_TIME_SECONDS,
            transaction_weights=TRANSACTION_WEIGHTS,
        ),
    )
    write_file(
        CONFIGS_DIR / f"execute_{warehouses}_warehouses_opt_rate.xml",
        OPT_RATE_XML.format(
            warehouses=warehouses,
            opt_rate=opt_rate,
            hostname=hostname,
            password=password,
            terminals=terminals,
            batch_size=BATCH_SIZE,
            benchmark_time=BENCHMARK_TIME_SECONDS,
            transaction_weights=TRANSACTION_WEIGHTS,
        ),
    )

    ramp_works = "".join([WORK_TEMPLATE.format(rate=rate) for rate in ramp_rates])
    write_file(
        CONFIGS_DIR / f"execute_{warehouses}_warehouses_ramp_up.xml",
        RAMP_UP_XML.format(
            warehouses=warehouses,
            works=ramp_works,
            hostname=hostname,
            password=password,
            terminals=terminals,
            batch_size=BATCH_SIZE,
        ),
    )

    # Loader config
    write_file(
        CONFIGS_DIR / f"load_{warehouses}_warehouses.xml",
        LOAD_XML.format(
            warehouses=warehouses,
            hostname=hostname,
            password=password,
            batch_size=BATCH_SIZE,
            loader_threads=LOADER_THREADS,
        ),
    )

    # Write scripts
    for suffix in ["max_rate", "opt_rate", "ramp_up"]:
        script = EXECUTE_SCRIPT.format(
            warehouses=warehouses, suffix=suffix, docker_image=docker_image
        )
        write_file(SCRIPTS_DIR / f"execute_{warehouses}_warehouses_{suffix}.sh", script)

    # Loader script
    write_file(
        SCRIPTS_DIR / f"load_{warehouses}_warehouses.sh",
        LOAD_SCRIPT.format(warehouses=warehouses, docker_image=docker_image),
    )

    print(f"Generated configs and scripts for {warehouses} warehouses and max rate {max_rate}.")


if __name__ == "__main__":
    main()

import shlex
from datetime import datetime, timedelta
import docker
from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator

HADOOP_CONTAINER = "hadoop-docker-itvdelab-1"
PROJECT_DIR = "/home/itversity/Looker_Ecommerce_Project"
OUTPUT_DIR = f"{PROJECT_DIR}/executed"

notebooks = {
    "bronze_layer":      f"{PROJECT_DIR}/bronze_layer.ipynb",
    "silver_layer":      f"{PROJECT_DIR}/silver_layer.ipynb",
    "gold_layer":        f"{PROJECT_DIR}/gold_layer.ipynb",
    "late_streaming_events": f"{PROJECT_DIR}/late_streaming_events.ipynb",
}

SOURCE_DIR = "/data/Raw_Looker_Ecommerce_BigQuery_Dataset"
SOURCE_FILES = ["users.csv", "products.csv", "distribution_centers.csv", "order_items.csv", "events.csv"]
FINGERPRINT_VARIABLE = "looker_ecommerce_source_fingerprint"
CHECK_TASK_ID = "Check_Hadoop_Container"

CELL_TIMEOUT = 1800 

default_args = {
    "email": ["karimashraf1309@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
}

def exec_in_hadoop(command: str, workdir: str = PROJECT_DIR) -> str:
    """Run `bash -lc <command>` inside the Hadoop container via the Docker socket
    (same as `docker exec -w <workdir> <container> bash -lc ...`), stream the output
    into the Airflow task log, fail the task if the exit code is not 0, and return the output."""
    client = docker.from_env()  
    container = client.containers.get(HADOOP_CONTAINER) 
    if container.status != "running":
        raise RuntimeError(f"Container {HADOOP_CONTAINER} is {container.status}, not running")

    api = client.api
    exec_id = api.exec_create(
        container.id, ["bash", "-lc", command], workdir=workdir, stdout=True, stderr=True
    )["Id"]
    output = []
    for chunk in api.exec_start(exec_id, stream=True):
        text = chunk.decode("utf-8", errors="replace")
        output.append(text)
        print(text, end="")

    code = api.exec_inspect(exec_id)["ExitCode"]
    if code != 0:
        raise RuntimeError(f"Command failed with exit code {code}: {command}")
    return "".join(output)


def check_hadoop_container():
    exec_in_hadoop(f"test -d {PROJECT_DIR} && command -v jupyter && command -v hdfs")


def get_source_fingerprint() -> str:
    """name, size and last-modified time of every source CSV, as one string"""
    files = " ".join(SOURCE_FILES)
    out = exec_in_hadoop(f"stat -c '%n %s %Y' {files}", workdir=SOURCE_DIR)
    lines = sorted(l.strip() for l in out.splitlines() if l.strip().split(" ")[0] in SOURCE_FILES)
    if len(lines) != len(SOURCE_FILES):
        raise RuntimeError(f"Expected {len(SOURCE_FILES)} source files in {SOURCE_DIR}, got: {lines}")
    return "\n".join(lines)


def check_source_changes(**context):
    """Branch: if no source CSV changed since the last successful run, only the late events
    task runs; otherwise the whole pipeline runs (bronze -> silver -> gold -> late events)."""
    check_hadoop_container()

    fingerprint = get_source_fingerprint()
    context["ti"].xcom_push(key="fingerprint", value=fingerprint)

    saved = Variable.get(FINGERPRINT_VARIABLE, default=None)
    if saved == fingerprint:
        print("Source files unchanged since the last successful run -> running only late_streaming_events.")
        return "Late_Streaming_Events_Kafka"

    print("Source files are new or changed -> running the full pipeline.")
    return "Bronze_Layer_Hdfs"


def run_notebook(name: str, notebook_path: str, ds: str, save_fingerprint: bool = False, **context) -> None:
    # Each run keeps its own executed copy (audit trail / data-quality report for that day).
    # The notebooks use `%run ./utils.ipynb`, so they run from their own folder (workdir).
    inner = (
        f"mkdir -p {OUTPUT_DIR} && "
        f"jupyter nbconvert --to notebook --execute "
        f"--ExecutePreprocessor.timeout={CELL_TIMEOUT} "
        f"--output-dir {OUTPUT_DIR} "
        f"--output {shlex.quote(f'{name}_{ds}_executed')} "
        f"{notebook_path}"
    )
    exec_in_hadoop(inner)

    if save_fingerprint:
        fingerprint = context["ti"].xcom_pull(task_ids=CHECK_TASK_ID, key="fingerprint")
        if fingerprint:
            Variable.set(FINGERPRINT_VARIABLE, fingerprint)
            print(f"Saved source fingerprint in Airflow Variable '{FINGERPRINT_VARIABLE}'.")
        else:
            print("No fingerprint found from the check task; nothing saved (next run will be a full run).")


with DAG(
    dag_id="looker_ecommerce_pipeline",
    description="run patching data of looker ecommerce (only when the source CSVs changed) and add late streaming events in fact_events",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 11, 10),
    catchup=False,
    max_active_runs=1,  
    dagrun_timeout=timedelta(hours=4), # 4 notebooks x (45 min limit + 1 retry + 5 min delay) must fit inside this
) as dag:

    start = EmptyOperator(task_id="Start")

    check_hadoop_container_task = BranchPythonOperator(
        task_id=CHECK_TASK_ID,
        python_callable=check_source_changes,
        retries=0,
    )

    bronze_layer = PythonOperator(
        task_id="Bronze_Layer_Hdfs",
        python_callable=run_notebook,
        op_kwargs={"name": "bronze_layer", "notebook_path": notebooks["bronze_layer"], "ds": "{{ ds }}"},
    )

    silver_layer = PythonOperator(
        task_id="Silver_Layer_Hdfs",
        python_callable=run_notebook,
        op_kwargs={"name": "silver_layer", "notebook_path": notebooks["silver_layer"], "ds": "{{ ds }}"},
    )

    gold_layer = PythonOperator(
        task_id="Gold_Layer_Hive",
        python_callable=run_notebook,
        op_kwargs={"name": "gold_layer", "notebook_path": notebooks["gold_layer"], "ds": "{{ ds }}",
                   "save_fingerprint": True},
    )

    late_streaming_events = PythonOperator(
        task_id="Late_Streaming_Events_Kafka",
        python_callable=run_notebook,
        op_kwargs={"name": "late_streaming_events", "notebook_path": notebooks["late_streaming_events"], "ds": "{{ ds }}"},
        trigger_rule="none_failed_min_one_success",
    )

    power_bi = EmptyOperator(task_id="Power_Bi") 

    end = EmptyOperator(task_id="End")

    start >> check_hadoop_container_task >> [bronze_layer, late_streaming_events]
    bronze_layer >> silver_layer >> gold_layer >> late_streaming_events >> power_bi >> end
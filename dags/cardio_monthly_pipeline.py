from datetime import datetime
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


PROJECT_PATH = os.environ.get(
    "PROJECT_PATH",
    "/host_mnt/c/Users/e.turchaninova/Downloads/cardio_project",
)

COMMON_MOUNTS = [
    Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
    Mount(source=f"{PROJECT_PATH}/data", target="/app/data", type="bind"),
    Mount(source=f"{PROJECT_PATH}/artifacts", target="/app/artifacts", type="bind"),
    Mount(source=f"{PROJECT_PATH}/logs", target="/app/logs", type="bind"),
    Mount(source=f"{PROJECT_PATH}/src", target="/app/src", type="bind"),
    Mount(source=f"{PROJECT_PATH}/.env", target="/app/.env", type="bind"),
]

COMMON_ENV = {
    "PYTHONPATH": "/app",
}


with DAG(
    dag_id="cardio_monthly_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 1 * *",
    catchup=False,
    tags=["cardio", "monthly"],
    description="Ежемесячный конвейер: извлечение, загрузка, silver, scoring, gold, визуализация",
) as dag:

    extract = DockerOperator(
        task_id="extract_eis_data",
        image="cardio/module_01_extract:latest",
        command=["python", "/app/src/modules/01_extract_eis.py"],
        docker_url="unix://var/run/docker.sock",
        network_mode="cardio_project_default",
        auto_remove="never",
        working_dir="/app",
        mounts=COMMON_MOUNTS,
        environment=COMMON_ENV,
        mount_tmp_dir=False,
        tty=True,
    )

    load_raw = DockerOperator(
        task_id="load_raw_to_postgres",
        image="cardio/module_02_load_raw:latest",
        command=["python", "/app/src/modules/02_load_raw_to_postgres.py"],
        docker_url="unix://var/run/docker.sock",
        network_mode="cardio_project_default",
        auto_remove="never",
        working_dir="/app",
        mounts=COMMON_MOUNTS,
        environment=COMMON_ENV,
        mount_tmp_dir=False,
        tty=True,
    )

    build_silver = DockerOperator(
        task_id="build_silver_layer",
        image="cardio/module_03_build_silver:latest",
        command=["python", "/app/src/modules/03_build_silver_layer.py"],
        docker_url="unix://var/run/docker.sock",
        network_mode="cardio_project_default",
        auto_remove="never",
        working_dir="/app",
        mounts=COMMON_MOUNTS,
        environment=COMMON_ENV,
        mount_tmp_dir=False,
        tty=True,
    )

    score_gold = DockerOperator(
        task_id="score_and_build_gold",
        image="cardio/module_07_score_gold:latest",
        command=["python", "/app/src/modules/07_score_and_build_gold.py"],
        docker_url="unix://var/run/docker.sock",
        network_mode="cardio_project_default",
        auto_remove="never",
        working_dir="/app",
        mounts=COMMON_MOUNTS,
        environment=COMMON_ENV,
        mount_tmp_dir=False,
        tty=True,
    )

    visualize = DockerOperator(
        task_id="visualize_results",
        image="cardio/module_08_visualize:latest",
        command=["python", "/app/src/modules/08_visualize_results.py"],
        docker_url="unix://var/run/docker.sock",
        network_mode="cardio_project_default",
        auto_remove="never",
        working_dir="/app",
        mounts=COMMON_MOUNTS,
        environment=COMMON_ENV,
        mount_tmp_dir=False,
        tty=True,
    )

    extract >> load_raw >> build_silver >> score_gold >> visualize

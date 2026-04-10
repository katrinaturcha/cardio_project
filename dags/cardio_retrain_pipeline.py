from datetime import datetime
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# Абсолютный путь НА ХОСТЕ, где лежит проект.
# Подставь свой реальный путь.
PROJECT_ROOT = os.environ.get("CARDIO_PROJECT_ROOT", "/opt/cardio_project")

COMMON_MOUNTS = [
    Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
    Mount(source=f"{PROJECT_ROOT}/data", target="/app/data", type="bind"),
    Mount(source=f"{PROJECT_ROOT}/artifacts", target="/app/artifacts", type="bind"),
    Mount(source=f"{PROJECT_ROOT}/logs", target="/app/logs", type="bind"),
    Mount(source=f"{PROJECT_ROOT}/src", target="/app/src", type="bind"),
    Mount(source=f"{PROJECT_ROOT}/.env", target="/app/.env", type="bind"),
]

COMMON_ENV = {
    "PYTHONPATH": "/app",
}

with DAG(
    dag_id="cardio_retrain_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["cardio", "retrain"],
) as dag:

    prepare_labeling = DockerOperator(
        task_id="prepare_labeling_file",
        image="cardio/module_04_prepare_labeling:latest",
        command=["python", "/app/src/modules/04_prepare_labeling.py"],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=COMMON_MOUNTS,
        environment=COMMON_ENV,
        working_dir="/app",
        mount_tmp_dir=False,
        auto_remove="never",
        tty=True,
        xcom_all=False,
    )

    load_labels = DockerOperator(
        task_id="load_labeled_data",
        image="cardio/module_05_load_labels:latest",
        command=["python", "/app/src/modules/05_load_labeled_data.py"],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=COMMON_MOUNTS,
        environment=COMMON_ENV,
        working_dir="/app",
        mount_tmp_dir=False,
        auto_remove="never",
        tty=True,
        xcom_all=False,
    )

    train_model = DockerOperator(
        task_id="train_models",
        image="cardio/module_06_train_model:latest",
        command=["python", "/app/src/modules/06_train_models.py"],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=COMMON_MOUNTS,
        environment=COMMON_ENV,
        working_dir="/app",
        mount_tmp_dir=False,
        auto_remove="never",
        tty=True,
        xcom_all=False,
    )

    prepare_labeling >> load_labels >> train_model

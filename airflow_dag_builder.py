from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow.utils.dates import days_ago
from datetime import timedelta, time
import datetime


class AirflowDAGBuilder:
    """
    Utility class for building Airflow DAGs with Bash, Python, and Dummy tasks,
    including support for dependencies and sensors.
    """

    def __init__(
        self,
        dag_id,
        description="",
        schedule_interval="0 0 * * *",
        start_days_ago=1,
        tags=None,
        max_active_runs=1,
        default_args=None,
        catchup=False
    ):
        """
        Initialize the DAG builder.

        Args:
            dag_id (str): Unique ID for the DAG.
            description (str): Description of the DAG.
            schedule_interval (str): CRON schedule.
            start_days_ago (int): Days ago to start.
            tags (list[str]): List of tags.
            max_active_runs (int): Max active DAG runs.
            default_args (dict): Airflow default_args.
            catchup (bool): Whether to catch up missed runs.
        """
        if tags is None:
            tags = []
        if default_args is None:
            default_args = {
                'owner': 'airflow',
                'depends_on_past': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
            }
        self.dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            description=description,
            schedule_interval=schedule_interval,
            start_date=days_ago(start_days_ago),
            tags=tags,
            catchup=catchup,
            max_active_runs=max_active_runs,
        )
        self.tasks = {}

    def add_bash_task(self, task_id, bash_command, env=None):
        """
        Add a BashOperator task.

        Args:
            task_id (str): Task ID.
            bash_command (str): Command to execute.
            env (dict): Environment variables.
        """
        task = BashOperator(
            task_id=task_id,
            bash_command=bash_command,
            env=env or {},
            dag=self.dag,
        )
        self.tasks[task_id] = task
        return task

    def add_python_task(self, task_id, python_callable, op_args=None, op_kwargs=None):
        """
        Add a PythonOperator task.

        Args:
            task_id (str): Task ID.
            python_callable (callable): Python function.
            op_args (list): Arguments.
            op_kwargs (dict): Keyword arguments.
        """
        task = PythonOperator(
            task_id=task_id,
            python_callable=python_callable,
            op_args=op_args or [],
            op_kwargs=op_kwargs or {},
            dag=self.dag,
        )
        self.tasks[task_id] = task
        return task

    def add_dummy_task(self, task_id):
        """
        Add a DummyOperator task.

        Args:
            task_id (str): Task ID.
        """
        task = DummyOperator(task_id=task_id, dag=self.dag)
        self.tasks[task_id] = task
        return task

    def add_time_sensor(self, task_id, target_time):
        """
        Add a TimeSensor task.

        Args:
            task_id (str): Task ID.
            target_time (datetime.time): Time to wait for.
        """
        task = TimeSensor(
            task_id=task_id,
            target_time=target_time,
            mode='poke',
            poke_interval=60 * 5,
            timeout=60 * 60 * 24,
            dag=self.dag
        )
        self.tasks[task_id] = task
        return task

    def set_dependencies(self, upstream_tasks, downstream_tasks):
        """
        Set dependencies between tasks.

        Args:
            upstream_tasks (str or list): Upstream task IDs.
            downstream_tasks (str or list): Downstream task IDs.
        """
        if isinstance(upstream_tasks, str):
            upstream_tasks = [upstream_tasks]
        if isinstance(downstream_tasks, str):
            downstream_tasks = [downstream_tasks]
        for u in upstream_tasks:
            for d in downstream_tasks:
                self.tasks[u] >> self.tasks[d]

    def get_dag(self):
        """
        Return the built DAG.

        Returns:
            airflow.DAG: The DAG object.
        """
        return self.dag

if __name__ == "__main__":
    # 1. Initialize builder
    dag_builder = AirflowDAGBuilder(
        dag_id="wifiScore_extenderScore",
        description="get wifiScore and extender Score",
        schedule_interval="00 09 * * *",
        start_days_ago=1,
        tags=["wifiScore", "extenderScore"],
        max_active_runs=1,
        default_args={
            'owner': 'ZheSun',
            'depends_on_past': False,
        },
        catchup=False
    )

    # 2. Add tasks
    wifiScore_task = dag_builder.add_bash_task(
        task_id="ClasswifiScore_v3",
        bash_command="/usr/apps/vmas/script/ZS/wifiScore/ClasswifiScore_v3.sh",
        env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'}
    )

    extender_list_task = dag_builder.add_bash_task(
        task_id="extender_list",
        bash_command="/usr/apps/vmas/script/ZS/wifiScore/extender_list.sh",
        env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'}
    )

    prepare_router_test_data_task = dag_builder.add_bash_task(
        task_id="prepare_router_test_data",
        bash_command="/usr/apps/vmas/script/ZS/wifiScore/prepare_router_test_data.sh",
        env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'}
    )

    extenderScore_task = dag_builder.add_bash_task(
        task_id="extenderScore",
        bash_command="/usr/apps/vmas/script/ZS/wifiScore/extenderScore.sh",
        env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'}
    )

    wait_for_1600_task = dag_builder.add_time_sensor(
        task_id='wait_for_1600',
        target_time=datetime.time(16, 0)
    )

    dummy_task = dag_builder.add_dummy_task(task_id="wifiScore_vmb_dummy")

    # 3. Set dependencies
    dag_builder.set_dependencies("ClasswifiScore_v3", ["prepare_router_test_data", "extender_list"])
    dag_builder.set_dependencies("ClasswifiScore_v3", "wait_for_1600")
    dag_builder.set_dependencies("wait_for_1600", "wifiScore_vmb_dummy")
    dag_builder.set_dependencies("extender_list", "extenderScore")
    dag_builder.set_dependencies("prepare_router_test_data", "extenderScore")

    # 4. Assign DAG object (required for Airflow to register the DAG)
    dag = dag_builder.get_dag()

    """
    Pipeline Architecture: wifiScore_extenderScore

    Start
    |
    |--> [ClasswifiScore_v3]
    |         |                     |
    |         v                     v
    |  [prepare_router_test_data]   [extender_list]
    |         |                     |
    |         v                     v
    |     [extenderScore] <---------
    |         
    |--> [wait_for_1600]
    |         |
    |         v
    |   [wifiScore_vmb_dummy]
    |
    End

    Legend:
    [TaskName] = Airflow task (usually BashOperator, DummyOperator, or TimeSensor)

    Dependencies:
    - ClasswifiScore_v3 triggers both prepare_router_test_data and extender_list in parallel.
    - Both prepare_router_test_data and extender_list must finish before extenderScore runs.
    - ClasswifiScore_v3 also triggers wait_for_1600, which, when finished, allows wifiScore_vmb_dummy to run.
    """


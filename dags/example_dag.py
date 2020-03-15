from datetime import datetime, timedelta
from random import randint

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator


def random_number():
	return randint(1, 10)

def branching(**context):
	value = context['task_instance'].xcom_pull(task_ids='some_logic')
	return "email" if value % 2 == 0 else "bash"


args = {
	"owner": "airflow",
	"depends_on_past": False,
	"start_date": datetime(2019, 3, 13),
	"retries": 0,
	"email_on_failure": True,
	"email": "salfetamen@yandex.ru"
}

with DAG("foo_dag", default_args=args, schedule_interval=timedelta(minutes=1), catchup=False) as dag:
	(
		PythonOperator(
			task_id="some_logic",
			python_callable=random_number
		)
		>> BranchPythonOperator(
			task_id="branching",
			provide_context=True,
			python_callable=branching
		)
		>> [
			EmailOperator(
				task_id="email",
				to="salfetamen@yandex.ru",
				subject="All good",
				html_content="Really good"
			),
			BashOperator(
				task_id="bash",
				bash_command="exit 1"
			)
		]
	)
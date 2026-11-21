import random
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

MY_NAME = "svitlana_moiseyenko"
CONNECTION_ID = "goit_mysql_db"

SOURCE_TABLE = "olympic_dataset.athlete_event_results"
TARGET_TABLE = f"{MY_NAME}.medal_counts"

MEDALS = ["Bronze", "Silver", "Gold"]
MAX_RECORD_AGE_SECONDS = 30


def pick_medal():
    """Випадково обирає тип медалі; значення потрапляє в XCom."""
    medal = random.choice(MEDALS)
    print(f"Обрано медаль: {medal}")
    return medal


def pick_medal_task(ti):
    """Повертає task_id завдання, що відповідає обраній медалі."""
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{medal}"


def generate_delay(params):
    """Затримує виконання наступного завдання на delay_seconds секунд."""
    delay = int(params["delay_seconds"])
    print(f"Затримка {delay} с")
    time.sleep(delay)


with DAG(
    dag_id=f"{MY_NAME}_medals",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    # 10 с — сенсор проходить; запуск із {"delay_seconds": 35} — сенсор падає
    params={"delay_seconds": 10},
    tags=[MY_NAME],
) as dag:
    # 1. Створення таблиці
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONNECTION_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # 2. Випадковий вибір медалі
    pick_medal_op = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    # 3. Розгалуження
    pick_medal_task_op = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=pick_medal_task,
    )

    # 4. Підрахунок записів з обраною медаллю та запис результату
    calc_tasks = [
        SQLExecuteQueryOperator(
            task_id=f"calc_{medal}",
            conn_id=CONNECTION_ID,
            sql=f"""
            INSERT INTO {TARGET_TABLE} (medal_type, count, created_at)
            SELECT '{medal}', COUNT(*), NOW()
            FROM {SOURCE_TABLE}
            WHERE medal = '{medal}';
            """,
        )
        for medal in MEDALS
    ]

    # 5. Затримка — запускається, якщо одне з трьох завдань успішне
    generate_delay_op = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # 6. Сенсор: найновіший запис не старший за 30 секунд
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=CONNECTION_ID,
        sql=f"""
        SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= {MAX_RECORD_AGE_SECONDS}
        FROM {TARGET_TABLE};
        """,
        mode="poke",
        poke_interval=5,
        timeout=15,
    )

    create_table >> pick_medal_op >> pick_medal_task_op >> calc_tasks
    calc_tasks >> generate_delay_op >> check_for_correctness

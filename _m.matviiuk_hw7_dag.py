from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
import random
import time

# 1: Функції
def random_medal_choice():
    return random.choice(["Gold", "Silver", "Bronze"])

def delay_execution():
    time.sleep(35)

def choose_branch(**kwargs):
    # pull XCom із таска select_medal
    medal = kwargs['ti'].xcom_pull(task_ids='select_medal')
    if medal == 'Gold':
        return 'count_gold'
    elif medal == 'Silver':
        return 'count_silver'
    else:
        return 'count_bronze'
    
# 2: Налаштування DAG
default_args = {"owner": "airflow", "start_date": days_ago(1)}
mysql_connection_id = "mysql_connection_mm"

with DAG(
    dag_id="mm_medal_counts_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["medal_pipeline"],
    description="Підрахунок медалей із затримкою та сенсором"
) as dag:

    # 3: Створення таблиці mm_medal_counts у схемі olympic_data
    create_medal_table = MySqlOperator(
        task_id="create_medal_table",
        mysql_conn_id=mysql_connection_id,
        sql="""
            CREATE TABLE IF NOT EXISTS mm_medal_counts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10),
                `count` INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        doc_md="### Створює таблицю mm_medal_counts"
    )

    # 4: Випадковий вибір медалі
    select_medal = PythonOperator(
        task_id="select_medal",
        python_callable=random_medal_choice,
        doc_md="### Випадковий вибір медалі"
    )

    # 5: Розгалуження за вибором медалі
    branching_task = BranchPythonOperator(
        task_id="branch_on_medal",
        python_callable=choose_branch,
        provide_context=True,
        doc_md="### Розгалуження за вибором медалі"
    )

    # 6: Підрахунок і вставка для Bronze
    count_bronze = MySqlOperator(
        task_id="count_bronze",
        mysql_conn_id=mysql_connection_id,
        sql="""
            INSERT INTO mm_medal_counts (medal_type, `count`)
            SELECT 'Bronze', COUNT(*)
              FROM olympic_dataset.athlete_event_results
             WHERE medal = 'Bronze';
        """,
        doc_md="### Підрахунок і вставка Bronze"
    )

    # 7: Підрахунок і вставка для Silver
    count_silver = MySqlOperator(
        task_id="count_silver",
        mysql_conn_id=mysql_connection_id,
        sql="""
            INSERT INTO mm_medal_counts (medal_type, `count`)
            SELECT 'Silver', COUNT(*)
              FROM olympic_dataset.athlete_event_results
             WHERE medal = 'Silver';
        """,
        doc_md="### Підрахунок і вставка Silver"
    )

    # 8: Підрахунок і вставка для Gold
    count_gold = MySqlOperator(
        task_id="count_gold",
        mysql_conn_id=mysql_connection_id,
        sql="""
            INSERT INTO mm_medal_counts (medal_type, `count`)
            SELECT 'Gold', COUNT(*)
              FROM olympic_dataset.athlete_event_results
             WHERE medal = 'Gold';
        """,
        doc_md="### Підрахунок і вставка Gold"
    )

    # 9: Затримка 35 секунд (якщо хоча б один count_* успішний)
    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=delay_execution,
        trigger_rule=TriggerRule.ONE_SUCCESS,
        doc_md="### Затримка 35 секунд"
    )

    # 10: Сенсор перевіряє, що останній запис не старший 30 секунд
    check_recent_record = SqlSensor(
        task_id="check_recent_record",
        conn_id=mysql_connection_id,
        sql="""
            SELECT (COUNT(*) > 0) AS has_recent
            FROM mm_medal_counts
            WHERE created_at >= NOW() - INTERVAL 30 SECOND;
        """,
        mode="poke",
        poke_interval=10,
        timeout=30,
        doc_md="### Перевіряє 'свіжість' запису ≤30 сек"
    )

    # 11: Послідовність виконання
    create_medal_table >> select_medal >> branching_task
    branching_task >> [count_bronze, count_silver, count_gold] >> delay_task >> check_recent_record

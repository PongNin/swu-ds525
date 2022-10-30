import json
import glob
import os
from typing import List

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def _create_tables():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_create_actor = """
        CREATE TABLE IF NOT EXISTS actor (
            actor_id BIGINT PRIMARY KEY,
            login VARCHAR (200) UNIQUE NOT NULL,
            display_login VARCHAR (200) UNIQUE NOT NULL,
            gravatar_id VARCHAR (200),
            url VARCHAR (255),
            avatar_url VARCHAR (255)
        )
    """
    table_create_repos = """
        CREATE TABLE IF NOT EXISTS repo (
            repo_id BIGINT PRIMARY KEY,
            name VARCHAR (255) UNIQUE NOT NULL,
            url VARCHAR (255)
        )
    """
    table_create_orgs = """
        CREATE TABLE IF NOT EXISTS org (
            orgs_id BIGINT PRIMARY KEY,
            login VARCHAR (255) UNIQUE NOT NULL,
            gravatar_id VARCHAR (225),
            url VARCHAR (255),
            avatar_url VARCHAR (255)        
        )
    """
    table_create_events = """
        CREATE TABLE IF NOT EXISTS events (
            events_id BIGINT PRIMARY KEY,
            actor_id BIGINT NOT NULL,
            repo_id BIGINT NOT NULL,
            type VARCHAR (200) NOT NULL,
            public BOOLEAN NOT NULL,
            created_at TIMESTAMP NOT NULL,
            orgs_id BIGINT,
            FOREIGN KEY (actor_id) REFERENCES actor (actor_id),
            FOREIGN KEY (repo_id) REFERENCES repo (repo_id),
            FOREIGN KEY (orgs_id) REFERENCES org (orgs_id)
        )
    """

    create_table_queries = [
        table_create_actor,table_create_repos,table_create_orgs,table_create_events
    ]

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def _drop_tables():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_drop_events = "DROP TABLE IF EXISTS events"
    table_drop_actor = "DROP TABLE IF EXISTS actor"
    table_drop_repos = "DROP TABLE IF EXISTS repo"
    table_drop_orgs = "DROP TABLE IF EXISTS orgs"

    drop_table_queries = [
    table_drop_events,table_drop_actor,table_drop_repos,table_drop_orgs
    ]

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def _process(**context):
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    ti = context["ti"]

    # Get list of files from filepath
    all_files = ti.xcom_pull(task_ids="get_files", key="return_value")
    # all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                
                if each["type"] == "IssueCommentEvent":
                    print(
                        each["id"], 
                        each["type"],
                        each["actor"]["id"],
                        each["actor"]["login"],
                        each["repo"]["id"],
                        each["repo"]["name"],
                        each["created_at"],
                        each["payload"]["issue"]["url"],
                    )
                else:
                    print(
                        each["id"], 
                        each["type"],
                        each["actor"]["id"],
                        each["actor"]["login"],
                        each["repo"]["id"],
                        each["repo"]["name"],
                        each["created_at"],
                    )

                # Insert data into tables here
                insert_statement = f"""
                    INSERT INTO actors (
                        id,
                        login
                    ) VALUES ({each["actor"]["id"]}, '{each["actor"]["login"]}')
                    ON CONFLICT (id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)

                # Insert data into tables here
                insert_statement = f"""
                    INSERT INTO events (
                        id,
                        type,
                        actor_id
                    ) VALUES ('{each["id"]}', '{each["type"]}', '{each["actor"]["id"]}')
                    ON CONFLICT (id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)

                conn.commit()


with DAG(
    "etl",
    start_date=timezone.datetime(2022, 10, 15),
    schedule="@daily",
    tags=["workshop"],
    catchup=False,
) as dag:

    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
        op_kwargs={
            "filepath": "/opt/airflow/dags/data",
        }
    )
    process = PythonOperator(
        task_id="process",
        python_callable=_process,
    )
    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )

    drop_tables = PythonOperator(
        task_id = "drop_tables",
        python_callable =_drop_tables,
    )

    get_files >> drop_tables >> create_tables >> process
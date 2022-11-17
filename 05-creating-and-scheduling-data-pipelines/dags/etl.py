import json
import glob
import os
from typing import List

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


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
        vatar_url VARCHAR (255)        
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

table_insert_actor = """
    INSERT INTO actor (
        actor_id,login,display_login,gravatar_id,url,avatar_url
    ) VALUES (%s,%s,%s,%s,%s,%s)
    ON CONFLICT (actor_id) DO NOTHING
"""
table_insert_repos = """
    INSERT INTO repo (
        repo_id,name,url
    ) VALUES (%s,%s,%s)
    ON CONFLICT (repo_id) DO NOTHING
"""
table_insert_orgs = """
    INSERT INTO org (
        orgs_id,login,gravatar_id,url,avatar_url
    ) VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT (orgs_id) DO NOTHING
"""

table_insert_events = """
    INSERT INTO events (events_id,actor_id,repo_id,type,public,created_at,orgs_id) VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (events_id) DO NOTHING
"""


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


def _create_tables(**context):
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def _etl(**context):
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    ti = context["ti"]

    # Get list of files from filepath
    all_files = ti.xcom_pull(task_ids="get_files", key="return_value")
    

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                #print(each["id"], each["type"], each["actor"]["login"])

                # Insert data into tables_actor
                actor_list=[]
                res_actor = []
                for actor in each["actor"]:
                    actor_list.append(actor)
                [res_actor.append(x) for x in actor_list if x not in res_actor]
                if each['actor'][res_actor[3]] !='':
                    col_actor= each['actor'][res_actor[0]],each['actor'][res_actor[1]],each['actor'][res_actor[2]],each['actor'][res_actor[3]],repr(each['actor'][res_actor[4]]),repr(each['actor'][res_actor[5]])
                else:
                    col_actor= each['actor'][res_actor[0]],each['actor'][res_actor[1]],each['actor'][res_actor[2]],"NULL",repr(each['actor'][res_actor[4]]),repr(each['actor'][res_actor[5]])
                cur.execute(table_insert_actor,col_actor)
                conn.commit()
                
                # Insert data into tables_repos
                repo_list=[]
                res_repo = []
                for repo in each["repo"]:
                    repo_list.append(repo)
                [res_repo.append(x) for x in repo_list if x not in res_repo]
                col_repo= each['repo'][res_repo[0]],each['repo'][res_repo[1]],each['repo'][res_repo[2]]
                cur.execute(table_insert_repos,col_repo)
                conn.commit()

                # Insert data into tables_orgs
                orgs_list=[]
                res_orgs = []
                try:
                    for org in each["org"]:
                        orgs_list.append(org)
                except:
                    pass
                [res_orgs.append(x) for x in orgs_list if x not in res_orgs]
                try:
                    if each['org'][res_orgs[2]] !='':
                        col_orgs= each['org'][res_orgs[0]],each['org'][res_orgs[1]],each['org'][res_orgs[2]],repr(each['org'][res_orgs[3]]),repr(each['org'][res_orgs[4]])
                    else:
                        col_orgs= each['org'][res_orgs[0]],each['org'][res_orgs[1]],"NULL",repr(each['org'][res_orgs[3]]),repr(each['org'][res_orgs[4]])
                    cur.execute(table_insert_orgs,col_orgs)
                    conn.commit()
                except:
                    pass
                
                # Insert data into events
                try:
                    col_events = each["id"],each['actor']["id"],each['repo']["id"],each["type"],each["public"],each["created_at"],each['org']["id"]
                except:
                    col_events = each["id"],each['actor']["id"],each['repo']["id"],each["type"],each["public"],each["created_at"],"NULL"
                cur.execute(table_insert_events,col_events)
                conn.commit()


with DAG(
    "etl",
    start_date=timezone.datetime(2022, 11, 10),
    schedule="@hourly",
    tags=["etl"],
    catchup=False,
) as dag:

    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
        op_kwargs={
            "filepath": "/opt/airflow/dags/data",
        }
    )

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )

    etl = PythonOperator(
        task_id="etl",
        python_callable=_etl,
    )


    get_files >> create_tables >> etl
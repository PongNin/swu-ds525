import glob
import json
import os
from typing import List

from cassandra.cluster import Cluster
from datetime import datetime


table_drop_events = "DROP TABLE events"
#table_drop_actors = "DROP TABLE actors"

table_create_events = """
    CREATE TABLE IF NOT EXISTS events
    (
        id text,
        type text,
        public boolean,
        created_at timestamp,
        actor_name text,
        actor_visit int,
        PRIMARY KEY (
            (created_at,
            id),
            type
        )
    )
"""

create_table_queries = [
    table_create_events
]
drop_table_queries = [
    table_drop_events
]

def drop_tables(session):
    for query in drop_table_queries:
        try:
            rows = session.execute(query)
        except Exception as e:
            print(e)


def create_tables(session):
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)


def get_files(filepath: str) -> List[str]:
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


def process(session, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            
            list_actor=[]
            counting_list={}
            for each in data:
                # Print some sample data
                #print(each["id"], each["type"], each["actor"]["login"])

                # Insert data into tables here
                query = f"""
                INSERT INTO events (id, type, public,created_at,actor_name) VALUES ('{each["id"]}', '{each["type"]}', {each["public"]}, '{each["created_at"]}','{each["actor"]["display_login"]}')
                """
                session.execute(query)
            


def insert_sample_data(session):
    query = f"""
    INSERT INTO events (id, type, public,created_at) VALUES ()
    """
    session.execute(query)


def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    # Create keyspace
    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS github_events
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """
        )
    except Exception as e:
        print(e)

    # Set keyspace
    try:
        session.set_keyspace("github_events")
    except Exception as e:
        print(e)

    drop_tables(session)
    create_tables(session)

    process(session, filepath="../Data")
    #insert_sample_data(session)

    # Select data in Cassandra and print them to stdout
    query = """
    SELECT * from events
    """
    #WHERE id = '23487929637' AND type = 'IssueCommentEvent' ALLOW FILTERING
    
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
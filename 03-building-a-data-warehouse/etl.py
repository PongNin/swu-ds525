import psycopg2


drop_table_queries = [
    "DROP TABLE IF EXISTS staging_events",
    "DROP TABLE IF EXISTS events",
    "DROP TABLE IF EXISTS actors",
    "DROP TABLE IF EXISTS repo",
    "DROP TABLE IF EXISTS org",
]

create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS staging_events (
        id text,
        type text,
        actor_name text,
        actor_id text,
        actor_url text,
        org_id text,
        org_login text,
        repo_id text,
        repo_name text,
        created_at text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        id text,
        type text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS actors (
        actor_id text,
        actor_name text,
        actor_url text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS org (
        org_id text,
        org_login text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS repo (
        repo_id text,
        repo_name text
    )
    """
]

copy_table_queries = [
    """
    COPY staging_events FROM 's3://ds525-github-event/github_events_01.json'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::766365816458:role/LabRole'
    JSON 's3://ds525-github-event/events_json_path.json'
    REGION 'us-east-1'
    """,
]

insert_table_queries = [
    """
    INSERT INTO
      events (
        id,
        type
      )
    SELECT
      DISTINCT id,type
    FROM
      staging_events
    WHERE
      id NOT IN (SELECT DISTINCT id FROM events)
    """,
    """
    INSERT INTO
      actors (
        actor_name,
        actor_id,
        actor_url
      )
    SELECT
      DISTINCT actor_name, actor_id, actor_url
    FROM
      staging_events
    WHERE
      actor_id NOT IN (SELECT DISTINCT actor_id FROM actors)
    """,
    """
    INSERT INTO
      org (
        org_id,
        org_login
      )
    SELECT
      DISTINCT org_id, org_login
    FROM
      staging_events
    WHERE
      org_id NOT IN (SELECT DISTINCT org_id FROM org)
    """,
    """
    INSERT INTO
      repo (
        repo_id,
        repo_name
      )
    SELECT
      DISTINCT repo_id, repo_name
    FROM
      staging_events
    WHERE
      repo_id NOT IN (SELECT DISTINCT repo_id FROM repo)
    """,
]


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    host = "redshift-cluster-1.ce1ofdjly7ll.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = "Ixaajph7"
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    query = "select * from events"
    cur.execute(query)
    records = cur.fetchall()
    for row in records:
        print(row)

    conn.close()


if __name__ == "__main__":
    main()
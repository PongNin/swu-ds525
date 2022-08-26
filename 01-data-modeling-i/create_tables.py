import psycopg2


table_drop_events = "DROP TABLE IF EXISTS events"
table_drop_actor = "DROP TABLE IF EXISTS actor"
table_drop_repos = "DROP TABLE IF EXISTS repo"
table_drop_orgs = "DROP TABLE IF EXISTS orgs"


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
drop_table_queries = [
    table_drop_events,table_drop_actor,table_drop_repos,table_drop_orgs
]


def drop_tables(cur, conn) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn) -> None:
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.
    - Establishes connection with the sparkify database and gets
    cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
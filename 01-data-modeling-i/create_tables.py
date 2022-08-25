import psycopg2


table_drop = "DROP TABLE IF EXISTS songplays"

table_create_actor = """
    CREATE TABLE IF NOT EXISTS actor (
        id serial PRIMARY KEY,
        login VARCHAR (200) UNIQUE NOT NULL,
        display_login VARCHAR (200) UNIQUE NOT NULL,
        gravatar_id serial,
        url VARCHAR (255),
        
    )
"""

create_table_queries = [
    table_create,
]
drop_table_queries = [
    table_drop,
]


def drop_tables(cur: PostgresCursor, conn: PostgresConn) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur: PostgresCursor, conn: PostgresConn) -> None:
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
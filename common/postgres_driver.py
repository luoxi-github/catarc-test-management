from contextlib import contextmanager

import psycopg2

from config.setting import POSTGRESQL

@contextmanager
def create_conn():
    """
    It creates a connection to the database, and then yields it to the caller. 
    
    The caller can then use the connection to execute queries. 
    
    When the caller is done, the connection is closed. 
    
    The connection is closed even if an exception is raised. 
    
    This is a very common pattern in Python. 
    
    It's called a context manager. 
    
    It's a way to ensure that resources are cleaned up when they're no longer needed. 
    
    The yield statement is what makes this a context manager. 
    
    It's a way to pass a value to the caller, and then get control back later on. 
    
    The caller uses the connection like this:
    """

    conn = psycopg2.connect(
        database=POSTGRESQL.get("database"),
        user=POSTGRESQL.get("user"),
        password=POSTGRESQL.get("password"),
        host=POSTGRESQL.get("host"),
        port=POSTGRESQL.get("port")
    )

    try:
        yield conn
    finally:
        try:
            conn.close()
        except:
            pass


def execute_sqls(conn, sqls):
    """
    It executes a list of SQL statements in a single transaction
    
    :param conn: the connection to the database
    :param sqls: a list of SQL statements to execute
    :return: A tuple of two values:
    1. A boolean value indicating whether the operation was successful or not.
    2. A string containing an error message if the operation failed.
    """

    try:
        if not isinstance(sqls, list):
            sqls = [sqls]

        with conn:
            for sql in sqls:
                with conn.cursor() as curs:
                    curs.execute(sql)

            conn.commit()

        return True, ""
    except Exception as e:
        return False, f"Caught exception: {e.__doc__}({e})"


def execute_many(conn, sql, data):
    """
    It takes a connection object, a SQL statement, and a list of tuples of data, and executes the SQL
    statement with the data
    
    :param conn: the connection object
    :param sql: The SQL statement to execute
    :param data: a list of tuples, each tuple is a row of data
    :return: A tuple of two values.
    """

    try:
        with conn:
            with conn.cursor() as curs:
                curs.executemany(sql, data)

            conn.commit()

        return True, ""
    except Exception as e:
        return False, f"Caught exception: {e.__doc__}({e})"


def fetch_one(conn, sql):
    """
    It takes a connection and a SQL statement as input, executes the SQL statement, and returns the
    result as a tuple.
    
    :param conn: The connection object
    :param sql: The SQL statement to execute
    :return: True, data
    """

    try:
        with conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                data = curs.fetchone()

        return True, data
    except Exception as e:
        return False, f"Caught exception: {e.__doc__}({e})"


def fetch_all(conn, sql):
    """
    It takes a connection and a SQL statement, executes the statement, and returns the results.
    
    :param conn: The connection object
    :param sql: The SQL statement to execute
    :return: True, data
    """

    try:
        with conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                data = curs.fetchall()

        return True, data
    except Exception as e:
        return False, f"Caught exception: {e.__doc__}({e})"
"""
Module has class which provide CRUD operations with PostgreSQL database
"""

import psycopg2

from config import config

class PostgreSQLClient(object):
    """ Helps the client work with PostgreSQL database

    """

    def __init__(self):
        #read parameters from database configuration
        params = config()
        try:
            self._db_conn = psycopg2.connect(**params)
            self._db_cur = self._db_conn.cursor()
        except psycopg2.DatabaseError as error:
            print error
        except Exception as error:
            print "Unhandled error: %s" % (error,)

    def __del__(self):
        self._db_conn.close()

    def insert(self, table, fields, values):
    """Build insert query with user parameters

    Args:
        table (str): table name
        fields (list or tuple): names of fields to insert values into
        values (list or tuple): values for inserting

    Returns:
        True: if operation was successful
        None: if operation wasn't successful
    """
        try:
            str_values = ','.join(("%s",) * len(values))
            query = "INSERT INTO %s (%s) VALUES (%s);" % (table,
                                                         ",".join(fields),
                                                         str_values)
            self._db_cur.execute(query, values)
            self._db_conn.commit()
            return True
        except psycopg2.DatabaseError as error:
            print error
        except Exception as error:
            print "Unhandled error: %s" % (error,)

    def update(self, table, set_data, filter_data):
    """Build update query with user parameters.
       Currently supports only one field to update with only one filter clause.

    Args:
        table (str): table name
        set_data (list or tuple): first element - field name to update,
                                  second element - new value
        filters_data (list or tuple): first element - field name to filter
                                      second element - filter value

    Returns:
        True: if operation was successful
        None: if operation wasn't successful
    """
        try:
            query = "UPDATE %s SET %s = %%s WHERE %s = %%s" %(table,
                                                              set_data[0],
                                                              filter_data[0])
            self._db_cur.execute(query, (set_data[1], filter_data[1]))
            self._db_conn.commit()
            return True
        except psycopg2.DatabaseError as error:
            print error
        except Exception as error:
            print "Unhandled error: %s" % (error,)

    def read(self, table, fields):
    """Build select query with user parameters

    Args:
        table (str): table name
        fields (list or tuple): names of fields to select

    Returns:
        rows (tuple of tuples): selected data
    """
        try:
            fields = ",".join(fields)
            self._db_cur.execute("SELECT %s FROM %s;" % (fields,table))
            rows = self._db_cur.fetchall()
            return rows
        except psycopg2.DatabaseError as error:
            print error
        except Exception as error:
            print "Unhandled error: %s" % (error,)

    def delete(self, table, filter_field, filter_val):
    """Build delete query with user parameters
       Currently supports only one filter clause.

    Args:
        table (str): table name
        filter_field (str): name of field to filter
        filter_value (str or int): filter value

    Returns:
        True: if operation was successful
        None: if operation wasn't successful
    """
        try:
            query = "DELETE FROM %s WHERE %s = %%s" %(table, filter_field)
            self._db_cur.execute(query, (filter_val,))
            self._db_conn.commit()
            return True
        except psycopg2.DatabaseError as error:
            print error
        except Exception as error:
            print "Unhandled error: %s" % (error,)


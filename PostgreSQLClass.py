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
        try:
            str_values = ','.join(("%s",) * len(values))
            query = "INSERT INTO %s (%s) VALUES (%s);" % (table,
                                                         ",".join(fields),
                                                         str_values)
            self._db_cur.execute(query, values)
            self._db_conn.commit()
            print "Data added successfully"
        except psycopg2.DatabaseError as error:
            print error
        except Exception as error:
            print "Unhandled error: %s" % (error,)

    def update(self, table, set_data, filter_data):
        try:
            query = "UPDATE %s SET %s = %%s WHERE %s = %%s" %(table,
                                                              set_data[0],
                                                              filter_data[0])
            self._db_cur.execute(query, (set_data[1], filter_data[1]))
            self._db_conn.commit()
            print "Data updated successfully"
        except psycopg2.DatabaseError as error:
            print error
        except Exception as error:
            print "Unhandled error: %s" % (error,)

    def read(self, table, fields):
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
        try:
            query = "DELETE FROM %s WHERE %s = %%s" %(table, filter_field)
            self._db_cur.execute(query, (filter_val,))
            self._db_conn.commit()
            print "Data deleted successfully"
        except psycopg2.DatabaseError as error:
            print error
        except Exception as error:
            print "Unhandled error: %s" % (error,)


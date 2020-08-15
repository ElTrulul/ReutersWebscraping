# Here all utils are stored
import sqlite3
import pandas as pd

def opendatabase(dbpath):
    connection = sqlite3.connect(dbpath)
    cursor = connection.cursor()
    print('Database created')
    return (connection, cursor)


def closedatabase(dbparameters):
    dbparameters[0].close()


def createtables(tablenames, dbparameters):
    for tablename in tablenames:
        sql_command = """DROP TABLE IF EXISTS {};""".format(tablename)
        dbparameters[1].execute(sql_command)
        sql_command = """CREATE TABLE IF NOT EXISTS {} (
                            ric TEXT,
                            timestamp INTEGER,
                            website TEXT,
                            html TEXT
                            );""".format(tablename)
        dbparameters[1].execute(sql_command)
    dbparameters[0].commit()
    print('Table(s) created')

    
def writedatabase(table, values, dbparameters):
    sql_command = '''INSERT INTO {}(ric, timestamp, website, html)
                  VALUES(?,?,?,?)'''.format(table)
    dbparameters[1].execute(sql_command, values)
    dbparameters[0].commit()

    
def writefeather(dataframe, location):
    dataframe = dataframe.reset_index() #feather does not support any other indexing than standard
    dataframe.to_feather(location)
    

def writecsv(dataframe, location):
    dataframe.to_csv(location, sep=";", decimal =",")
    

def sqlquery(connection, table):
    sql_command = sql_command = '''SELECT * FROM {0}'''.format(table)
    return pd.read_sql_query(sql_command, connection)

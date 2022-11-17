
import os
import pyodbc
import pandas as pd
from tantalis_bigQuery import load_sql

# USE THIS WITHIN DESKTOP
def connect_to_DB (driver,server,port,dbq, username,password):
    """ Returns a connection to Oracle database"""
    try:
        connectString ="""
                    DRIVER={driver};
                    SERVER={server}:{port};
                    DBQ={dbq};
                    Uid={uid};
                    Pwd={pwd}
                       """.format(driver=driver,server=server, port=port,
                                  dbq=dbq,uid=username,pwd=password)

        connection = pyodbc.connect(connectString)
        print  ("...Successffuly connected to the database")
    except:
        raise Exception('...Connection failed! Please check your connection parameters')

    return connection


def read_query(connection,query):
    "Returns a df containing SQL Query results"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        cols = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame.from_records(rows, columns=cols)
    
    finally:
        if cursor is not None:
            cursor.close()

            
   # USE THIS OUTSIDE OF DESKTOP
connection_string ='''Driver={driver};
            DBQ={hostname}:{sid};
            UID={username};
            PWD={password};
            Authentication=LDAP;
            Trusted_Connection="yes"
                 '''.format(driver = '{Oracle in OraClient12Home1}', 
                            hostname = 'bcgw.bcgov/idwprod1.bcgov', 
                            sid = 'IDWPROD1',
                            username = 'XXX',
                            password = 'XXX')

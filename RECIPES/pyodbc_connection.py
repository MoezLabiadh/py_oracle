import json
import pyodbc
import pandas as pd


def get_db_cnxinfo (dbname='BCGW'):
    """ Retrieves db connection params from the config file"""
    
    with open(r'H:\config\db_config.json', 'r') as file:
        data = json.load(file)
        
    if dbname in data:
        cnxinfo = data[dbname]
        return cnxinfo
    
    raise KeyError(f"Database '{dbname}' not found.")
     
    
# USE THIS WITHIN DESKTOP
def connect_to_DB (driver,server,port,dbq, username,password):
    """ Returns a connection to Oracle database"""
    try:
        connectString =f"""
                    DRIVER={driver};
                    SERVER={server}:{port};
                    DBQ={dbq};
                    Uid={username};
                    Pwd={password}
                       """
        connection = pyodbc.connect(connectString)
        cursor= connection.cursor()
        print  ("...Successffuly connected to the database")
    except:
        raise Exception('...Connection failed! Please check your connection parameters')

    return connection, cursor



            
# USE THIS OUTSIDE OF DESKTOP
def connect_to_DB (driver,hostname,sid,username,password):
    """ Returns a connection to Oracle database"""
    try:
        connection_string =f"""
                    Driver={driver};
                    DBQ={hostname}:{sid};
                    UID={username};
                    PWD={password};
                    Authentication=LDAP;
                         """
        connection = pyodbc.connect(connection_string)
        cursor= connection.cursor()
        print  ("...Successffuly connected to the database")
    except:
        raise Exception('...Connection failed! Please check your connection parameters')

    return connection, cursor



def read_query(connection, cursor, query, bvars):
    "Returns a df containing SQL Query results"
    try:
        cursor.execute(query, bvars)
        columns = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=[x for x in columns])
        
        return df
    
    finally:
        cursor.close()
        connection.close() 
        


driver= [x for x in pyodbc.drivers() if x.startswith('Oracle')][0]  
cnxinfo= get_db_cnxinfo(dbname='BCGW')
server = cnxinfo['server']
port= cnxinfo['port']
dbq= cnxinfo['dbq']
username= cnxinfo['username']
password= cnxinfo['password']

connection, cursor= connect_to_DB (driver,server,port,dbq,username,password)

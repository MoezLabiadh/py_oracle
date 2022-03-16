import os
import cx_Oracle
import pandas as pd

def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("Successffuly connected to the database")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection


def read_query(connection,query):
    "Returns a df containing results of SQL Query "
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        names = [ x[0] for x in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=names)
    
    finally:
        if cursor is not None:
            cursor.close()
            
def main ():
    print (" Connect to BCGW")    
    bcgw_host = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,bcgw_host)
    
    print ("Execute SQL Query")
    
    sql = """
            SELECT*
            
             FROM
                 WHSE_TANTALIS.TA_CROWN_TENURES_SVW ten
                 INNER JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                   ON SDO_ANYINTERACT (pip.SHAPE, ten.SHAPE) = 'TRUE'
            
            WHERE ten.TENURE_PURPOSE = 'AQUACULTURE'
              AND ten.TENURE_SUBPURPOSE = 'PLANTS'
              AND ten.TENURE_STATUS = 'DISPOSITION IN GOOD STANDING'
            """

    df = read_query(connection,sql)
    print (df.shape[0])
    
    
main ()

import os
import cx_Oracle
import geopandas as gpd
from shapely import wkb



def connect_to_DB (username,password,hostname):
    """ Returns a connection and cursor to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("....Successffuly connected to the database")
    except:
        raise Exception('....Connection failed! Please check your login parameters')

    return connection



def load_sql():
    """Returns a dict of queries to execute"""
    
    sql= {}
    sql['ff'] = """
                SELECT ct.CROWN_LANDS_FILE, ct.TENURE_STAGE,
                       SDO_UTIL.TO_WKBGEOMETRY(ct.SHAPE) AS SHAPE
                       
                FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW ct
                
                WHERE ct.TENURE_SUBPURPOSE = 'FIN FISH'
                    """
    return sql



def oracle_to_gdf (connection, sql):
    """ Returns a gdf containing the query results"""
    cursor = connection.cursor()
    cursor.execute(sql)
    
    result = cursor.fetchall()

    columns = [desc[0] for desc in cursor.description]
    gdf = gpd.GeoDataFrame(result, columns=columns)
    

    gdf['geometry'] = gdf['SHAPE'].apply(lambda x: wkb.loads(x.read()))
    del gdf['SHAPE']

    cursor.close()
    connection.close()
    
    return gdf
    
    
    
print ('Connecting to BCGW.')
hostname = 'bcgw.bcgov/idwprod1.bcgov'
bcgw_user = os.getenv('bcgw_user')
bcgw_pwd = os.getenv('bcgw_pwd')
connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)

print ('Running the SQL.')
sql= load_sql()
gdf = oracle_to_gdf (connection, sql['ff'])

def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("Successffuly connected to the database")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection
  
  
  def load_queries ():
    """ Return the SQL queries that will be executed"""
    sql = {}
    sql['fn'] = """ 
                SELECT  
                  ipr.INTRID_SID, pip.CNSLTN_AREA_NAME, pip.CONTACT_ORGANIZATION_NAME, pip.CONTACT_NAME, 
                  pip.ORGANIZATION_TYPE, pip.CONTACT_UPDATE_DATE, pip.CONTACT_TITLE, pip.CONTACT_ADDRESS, 
                  pip.CONTACT_CITY, pip.CONTACT_PROVINCE, pip.CONTACT_POSTAL_CODE,
                  pip.CONTACT_FAX_NUMBER, pip.CONTACT_PHONE_NUMBER, pip.CONTACT_EMAIL_ADDRESS,
                  SDO_UTIL.TO_WKTGEOMETRY(ipr.SHAPE) SHAPE
                
                FROM
                WHSE_TANTALIS.TA_CROWN_TENURES_SVW ipr
                    INNER JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip 
                        ON SDO_RELATE (pip.SHAPE, ipr.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                
                WHERE ipr.INTRID_SID IN (XXX)
                """
    return sql
  
  
  def read_query(connection,query):
    "Returns a df containing SQL Query results"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        names = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
                        
        return pd.DataFrame(rows, columns=names)
    
    finally:
        if cursor is not None:
            cursor.close()
            
            
def df_2_gdf (df, crs):
    """ Return a geopandas gdf based on a df with Geometry column"""
    df['SHAPE'] = df['SHAPE'].astype(str)
    df['geometry'] = df['SHAPE'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry = df['geometry'])
    gdf.crs = "EPSG:" + str(crs)
    del df['SHAPE']
    
    return gdf

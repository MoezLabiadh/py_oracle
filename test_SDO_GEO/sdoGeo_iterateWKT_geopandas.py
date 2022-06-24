import os
import geopandas as gpd
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

def get_wkt_srid (fc):
    """Returns the SRID and WKT string of each feature of a shapefile"""
    gdf = gpd.read_file(fc)
    gdf['wkt'] = gdf.apply(lambda row:row['geometry'].wkt, axis=1)
    srid = gdf.crs.to_epsg()
    
    wkt_dict = {}
    
    for index, row in gdf.iterrows():
        wkt = row['wkt']
        if len (wkt) <= 4000:
            print ('WKT returned for feature {} - under 4000 characters '.format (str(index)))
            f = 'feature '+ str(index) # Replace index with another ID column (name ?)
            wkt_dict [f] = wkt
        else:
            print ('WKT NOT returned for feature {} more than 4000 characters'.format (str(index)))
            continue

    return wkt_dict, srid


def read_query(connection,query):
    "Returns a df containing results of SQL Query "
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        names = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=names)
    
    finally:
        if cursor is not None:
            cursor.close()
            
def generate_report (workspace, df_list, sheet_list, filename):
    """ Exports dataframes to multi-tab excel spreasheet"""
    out_file = os.path.join(workspace, str(filename) + '.xlsx')

    writer = pd.ExcelWriter(out_file,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'count'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()


def main ():
    wks = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\TOOLS\SCRIPTS\RECIPES\WKT_geoPandas'
    fc = os.path.join(wks, 'test.shp')
    
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    
    print ('Connecting to BCGW')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ('Getting WKT')
    
    wkt_dict, srid = get_wkt_srid (fc)
    
    
    sql =  """
            SELECT*
            FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW t
            WHERE t.TENURE_SUBPURPOSE = 'FIN FISH'
            AND t.TENURE_STAGE = 'TENURE'
            AND SDO_RELATE (t.SHAPE, SDO_GEOMETRY('{w}', {s}),
                            'mask=ANYINTERACT') = 'TRUE'
            """
    
    print ('Running SQL')
    dfs = []
    keys = []
    
    for k, v in wkt_dict.items():
        query = sql.format(w = v,  s = srid)
        df = read_query(connection,query)
        dfs.append(df)
        keys.append(k)
    
    sheets = ['Intersect Feature ' + k for k in keys]
    
    generate_report (wks, dfs, sheets, 'Query_Results')


main ()

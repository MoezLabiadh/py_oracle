import os
import pandas as pd
import cx_Oracle
import geopandas as gpd

#from datetime import datetime

aoi = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\TOOLS\SCRIPTS\STATUSING\test_data\aoi_test_7_vvvbig.shp'
#aoi = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\TOOLS\SCRIPTS\STATUSING\test_data\aoi_test_4_big.shp'

sql  = """
                    SELECT b.INTRID_SID, b.CROWN_LANDS_FILE, b.TENURE_SUBPURPOSE, SDO_UTIL.TO_WKTGEOMETRY(b.SHAPE) SHAPE
                    
                    FROM  WHSE_TANTALIS.TA_CROWN_TENURES_SVW b
                    
                    WHERE SDO_RELATE (b.SHAPE, 
                                      SDO_GEOMETRY(:wkb, :srid),'mask=ANYINTERACT') = 'TRUE'
        """
        
        
        
hostname = 'bcgw.bcgov/idwprod1.bcgov'
username = os.getenv('bcgw_user')
password = os.getenv('bcgw_pwd')

connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
cursor = connection.cursor()

def esri_to_gdf (aoi):
    """Returns a Geopandas file (gdf) based on 
       an ESRI format vector (shp or featureclass/gdb)"""
    
    if '.shp' in aoi: 
        gdf = gpd.read_file(aoi)
    
    elif '.gdb' in aoi:
        l = aoi.split ('.gdb')
        gdb = l[0] + '.gdb'
        fc = os.path.basename(aoi)
        gdf = gpd.read_file(filename= gdb, layer= fc)
        
    else:
        raise Exception ('Format not recognized. Please provide a shp or featureclass (gdb)!')
    
    return gdf

gdf = esri_to_gdf (aoi)

srid = gdf.crs.to_epsg()
wkb = gdf['geometry'].to_wkb().iloc[0] 
wkt = gdf['geometry'].to_wkt().iloc[0] 

print (len(wkt))

cursor.setinputsizes(wkb=cx_Oracle.BLOB)

           
bvars = {'wkb': wkb, 'srid': srid} 
cursor.execute(sql,bvars)

rows = cursor.fetchall()
colnames = [x[0] for x in cursor.description]
df = pd.DataFrame(rows, columns=colnames)

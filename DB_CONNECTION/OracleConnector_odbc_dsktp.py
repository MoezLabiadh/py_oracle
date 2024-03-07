import json
import pyodbc
import pandas as pd

class OracleConnector:
    def __init__(self, dbname='BCGW'):
        self.dbname = dbname
        self.cnxinfo = self.get_db_cnxinfo()

    def get_db_cnxinfo(self):
        """ Retrieves db connection params from the config file"""
        with open(r'H:\config\db_config.json', 'r') as file:
            data = json.load(file)
        
        if self.dbname in data:
            return data[self.dbname]
        
        raise KeyError(f"Database '{self.dbname}' not found.")

    def connect_to_db(self):
        """ Connects to Oracle DB and create a cursor"""
        try:
            driver= [x for x in pyodbc.drivers() if x.startswith('Oracle')][0]  
            self.connection_string =f"""
                        DRIVER={driver};
                        SERVER={self.cnxinfo['server']}:{self.cnxinfo['port']};
                        DBQ={self.cnxinfo['dbq']};
                        Uid={self.cnxinfo['username']};
                        Pwd={self.cnxinfo['password']}
                        """
            self.connection = pyodbc.connect(self.connection_string)
            self.cursor= self.connection.cursor()
            print  ("..Successffuly connected to the database")
        except Exception as e:
            raise Exception('..Connection failed:', e)
            

    def disconnect_db(self):
        """Close the Oracle connection and cursor"""
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            print("....Disconnected from the database")

if __name__ == "__main__":
    oracle_connector = OracleConnector()
    oracle_connector.connect_to_db()
    
    #test
    try:
        query = """
        SELECT
            CROWN_LANDS_FILE,
            TENURE_STATUS,
            ROUND(TENURE_AREA_IN_HECTARES, 2) AS AREA_HA,
            SDO_UTIL.TO_WKTGEOMETRY(SHAPE) AS GEOMETRY
        FROM 
            WHSE_TANTALIS.TA_CROWN_TENURES_SVW
        WHERE 
            TENURE_SUBPURPOSE = 'PRIVATE MOORAGE'
            AND TENURE_STAGE = 'APPLICATION' 
            AND RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
                """
        df = pd.read_sql(query, oracle_connector.connection)

    except Exception as e:
        raise Exception(f"Error occurred: {e}")
    
    finally:
        oracle_connector.disconnect_db()

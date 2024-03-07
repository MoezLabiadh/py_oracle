import json
import cx_Oracle
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
            self.connection = cx_Oracle.connect(self.cnxinfo['username'], 
                                                self.cnxinfo['password'], 
                                                self.cnxinfo['hostname'], 
                                                encoding="UTF-8")
            self.cursor = self.connection.cursor()
            print  ("..Successffuly connected to the database")
        except Exception as e:
            raise Exception(f'..Connection failed: {e}')

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
                SELECT*
                FROM WHSE_FOREST_VEGETATION.PEST_INFESTATION_POLY
                FETCH FIRST 20 ROWS ONLY
                """
        df = pd.read_sql(query, oracle_connector.connection)

    except Exception as e:
        raise Exception(f"Error occurred: {e}")
    
    finally:
        oracle_connector.disconnect_db()

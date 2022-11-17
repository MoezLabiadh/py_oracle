
import pyodbc

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

# USE THIS WITHIN DESKTOP
connectString = """
        DRIVER={Oracle in OraClient12Home1};
        SERVER=bcgw.bcgov:1521;
        DBQ=idwprod1;
        Uid=XXX;
        Pwd=XXX
        """

#connect
cnxn = pyodbc.connect(connectString)

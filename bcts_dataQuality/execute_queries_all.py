##-------------------------------------------------------------------------------
# Name:     Data Quality Checker tool

# Purpose: This module runs SQL queries against LRM database
#          and spreadhsheets exported from CAS financial system.
#          The objective is to retrieve the major PR non-conformances.
#
#
# Author:  Moez Labiadh, BCTS-TKO
#
# Created:       15-12-2020
# Last updated:  25-02-2021
#-------------------------------------------------------------------------------


import os
import sys
import cx_Oracle
import pandas as pd
import numpy as np
import xlsxwriter
from datetime import date

from Queries import make_sql_string, build_sql_query
from UBI_lookup import get_UBI_prefix


def connect_to_DB (username,password,hostname):
    """ Returns a connection to LRM database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("Successffuly connected to LRM database")
    except:
        raise Exception('Connection to LRM mapview failed! Please verifiy your login parameters')

    return connection


def get_ubi_cost (ubi_report_lt, ubi_report_st,BA):
    """ Retrieves UBIs with layout cost
    based on spreadsheets exported from CAS system"""
    #ignore override warnings
    pd.options.mode.chained_assignment = None

    #convert reports to df
    print  ("Running queries...")
    df_lt = pd.read_excel(ubi_report_lt)
    df_st = pd.read_excel(ubi_report_st)

    #harmonize column names and types
    cols = ['UBI', 'Service_Line', 'FY' , 'Amount']
    df_lt['FY'] = df_lt['Fiscal'].str[2:].astype(float)
    df_lt = df_lt[['UBI/URI', 'Service Line' , 'FY', 'Actuals YTD + Yr Open']]
    df_lt.columns = cols

    df_st['FY'] = 0.0
    df_st = df_st[['Project', 'Service Line', 'FY',  'Amount']]
    df_st.columns = cols
    df_st['UBI'] = df_st['UBI'].str[2:]

    #keep only selected BA UBI and costs after 2019.
    ubi_prefix = get_UBI_prefix (BA)

    if ubi_prefix == 'B':
        df_lt_bf = df_lt.loc[(df_lt['UBI'].str[0:1] == ubi_prefix) &
                             (df_lt['FY'] >= 19)]

        df_st_bf = df_st.loc[df_st['UBI'].str[0:1] == ubi_prefix]

    else:
        df_lt_bf = df_lt.loc[(df_lt['UBI'].str[0:2] == ubi_prefix) &
                             (df_lt['FY'] >= 19)]

        df_st_bf = df_st.loc[df_st['UBI'].str[0:2] == ubi_prefix]

    #concatinate the two reports into a single df
    df_all = pd.concat([df_lt_bf,df_st_bf], axis=0)

    if (df_all.shape[0] == df_lt_bf.shape[0] + df_st_bf.shape[0]) == False:
        print ('There is a problem with the df concatination!')

    #keep only the "Block Layout" service lines
    df_all_layout = df_all.loc[(df_all['Service_Line'] == 42469)
                             | (df_all['Service_Line'] == 42467)
                             | (df_all['Service_Line'] == 42472)]

    #calculate total layout cost for each ubi/block
    df_all_layout['Total_per_UBI'] = df_all_layout.groupby('UBI')['Amount'].transform(np.sum)
    df_all_layout = df_all_layout.drop_duplicates(subset=['UBI'])

    #select all UBIs with money (>$0) spent on layout
    df_all_layout_costs = df_all_layout.loc[df_all_layout['Total_per_UBI'] > 0]

    #get the final list of UBIs
    ubi_list = df_all_layout_costs ['UBI'].tolist()

    return ubi_list


def run_query (DB_connection, query_dict, out_excel):
    """Runs the SQL queries and export reports to excel"""
    #create a spreadsheet
    writer = pd.ExcelWriter(out_excel, engine='xlsxwriter')

    df0 = pd.DataFrame()
    #export each sql query result into a seperate sheet
    for k,v in query_dict.items():
            df = pd.read_sql(v, con=DB_connection)
            df['ErrorCode'] = k
            df['UNIQUE_KEY'] = df['LICENCE_ID'] + "-" + df['BLOCK_ID']
            df0 = pd.concat([df0,df], axis=0, ignore_index=True)


    cols = ['TSO_CODE','LICENCE_ID','BLOCK_ID','UNIQUE_KEY','ErrorCode']
    df0 = df0[cols]

    #grouped = df0.groupby(['TSO_CODE','ErrorCode'])['UNIQUE_KEY'].count()
    #df_gr =pd.DataFrame(grouped)
    #df_gr.rename(columns={"UNIQUE_KEY": "ErrorCount"}, inplace = True)
    df0.to_excel(writer,sheet_name='ALL', encoding='utf-8')


    writer.save()
    writer.close()
    DB_connection.close()

def main ():
    # BA and LRM connextion parameters
    hostname = 'lrmbctsp.nrs.bcgov/DBP06.nrs.bcgov'
    BA = 'ALL'
    username = raw_input("Enter your LRM username : ")
    password = raw_input("Enter your LRM password : ")

     #get the UBI cost reports exported from CAS *******LINKS TO CAS REPORTS NEED TO BE UPDATED REGUARLY********
    ubi_report_lt = r'\\forwebfiles.nrs.bcgov\ftp\HBT\external\!publish\Web\Financial\archive\UBIs or URIs with $ in CAS FY04 to FY20.xlsx'
    ubi_report_st = r'\\bctsdata\data\!shared_root\05_GIS\05_Tasks\T2021_45_KPIErrorsTool\Data\2020_APR1_2021_FEB26.xlsx'

    today = date.today().strftime("%Y%m%d")
    workspace = r'\\bctsdata\data\!shared_root\05_GIS\05_Tasks\T2021_45_KPIErrorsTool\Output\KPI_error_checker'
    out_excel = os.path.join(workspace, 'qa_report_' + today + '_ALL_forDashboard.xlsx')

    # Run functions
    DB_connection = connect_to_DB (username,password,hostname)
    ubi_list = get_ubi_cost(ubi_report_lt, ubi_report_st,BA)
    str_list = make_sql_string (BA,ubi_list)
    query_dict = build_sql_query (str_list)
    run_query (DB_connection, query_dict,out_excel)
    print  ("Report exported...")


if __name__ == "__main__":
    main()
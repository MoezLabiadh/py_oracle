import os
import sys
import arcpy
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
        arcpy.AddMessage  ("Successffuly connected to LRM database")
    except:
        raise Exception('Connection to LRM mapview failed! Please verifiy your login parameters')

    return connection


def get_ubi_cost (ubi_report_lt, ubi_report_st,BA):
    """ Retrieves UBIs with layout cost
    based on spreadsheets exported from CAS system"""
    #ignore override warnings
    pd.options.mode.chained_assignment = None

    #convert reports to df
    arcpy.AddMessage  ("Importing data from CAS spreadsheets...this might take a while!")
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
        arcpy.addWarning ('There is a problem with the df concatination!')

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


def run_query (DB_connection, query_dict, select_queries_list, out_excel):
    """Runs the SQL queries and export reports to excel"""
    #create a spreadsheet
    if len(select_queries_list)> 0:
        writer = pd.ExcelWriter(out_excel, engine='xlsxwriter')
    else:
        raise Exception ('No Queries Selected!')

    #export each sql query result into a seperate sheet
    for k,v in query_dict.items():
        if k in select_queries_list:
            df = pd.read_sql(v, con=DB_connection)
            rows = df.shape[0]
            cols = df.shape[1]

            if rows < 1:
                arcpy.AddWarning ('{} table is empty: no report exported!' .format (k))
            else:
                df = df.reset_index(drop=True)
                df.index = df.index + 1
                df.index.name = '#'

                df.to_excel(writer,sheet_name=k, encoding='utf-8')
                arcpy.AddMessage ('{} report has {} rows' .format(k,df.shape[0]))

                #format sheets
                workbook = writer.book
                worksheet = writer.sheets[k]
                format = workbook.add_format({'border':1, 'text_wrap': True})
                worksheet.conditional_format( 0,0,rows,cols,{'type':'no_blanks','format':format})
                worksheet.conditional_format( 0,0,rows,cols,{'type':'blanks','format':format})
                worksheet.set_column(1, 7, 12)
                worksheet.set_column(8, cols, 21)


    writer.save()
    writer.close()
    DB_connection.close()


def main ():
    # BA and LRM connextion parameters
    hostname = 'lrmbctsp.nrs.bcgov/DBP06.nrs.bcgov'
    BA = sys.argv[1] # choose from list
    username = sys.argv[2] # LRM mapview username
    password = sys.argv[3] # LRM mapview password

     #get the UBI cost reports exported from CAS
    cas_reports = sys.argv[4]
    ubi_report_lt = cas_reports.split(';')[0].translate(None,"'")
    ubi_report_st = cas_reports.split(';')[1].translate(None,"'")

    #get list of selected queries from user inputs
    select_queries = sys.argv[5]
    select_queries_list = select_queries.split(';')

    today = date.today().strftime("%Y%m%d")
    workspace = sys.argv[6]
    out_excel = os.path.join(workspace, 'qa_report_' + today + '_' + BA + '.xlsx')

    # Run functions
    DB_connection = connect_to_DB (username,password,hostname)
    ubi_list = get_ubi_cost(ubi_report_lt, ubi_report_st,BA)
    str_list = make_sql_string (BA,ubi_list)
    query_dict = build_sql_query (str_list)
    run_query (DB_connection, query_dict, select_queries_list,out_excel)


if __name__ == "__main__":
    main()

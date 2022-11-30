#5-2 New ODS Table
def new_ods_table(ETL_Directory, Inbound_File_Data, credentials):

    print('Executing new_ods_table.py, 5-2')
    import pandas as pd
    import numpy as np
    import pyodbc
    
    #variable instantiation
    SQL = ""
    
    #5-2a: pull the ODS_Table from ETL_Directory
    print('Executing 5-2a')
    ods_table = ETL_Directory['ODS_Table'][0]
    
    #5-2b: iterate through each column, find the max length, and make part of the SQL string
    print('Executing 5-2b')
    for col in Inbound_File_Data.columns:
        if max(Inbound_File_Data[col].astype(str).str.len()) == 0: #enforce VARCHAR(1) when the max length is 0
            SQL = SQL + col + " VARCHAR(1),"
        else:
            SQL = SQL + col + " VARCHAR(" + str(max(Inbound_File_Data[col].astype(str).str.len())) + "), "
        
    #5-2c: write out the SQL string for a CREATE TABLE statement, remove the extra 2 characters, ", ", at the end of the SQL string
    print('Executing 5-2c')
    SQL = f"CREATE TABLE {ods_table} (" + SQL[:-2] + ") ON [PRIMARY];"
        
    #5-2d: execute the SQL string in EDW_ODS
    print('Executing 5-2d')
    
    #variable instantiation
    server = credentials[0]
    database = credentials[1]
    username = credentials[2]
    password = credentials[3]
    
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.fast_executemany = True

    #create ODS table, will print SQL error results if there are any
    print(SQL)
    cursor.execute(SQL)
    
    cursor.commit()
    cursor.close()
    conn.close()
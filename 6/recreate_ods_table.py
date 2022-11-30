#6-2 Recreate ODS Table
def new_ods_table(ETL_Directory, Inbound_File_Data, credentials):

    print('Executing new_ods_table.py, 6-2')
    import pandas as pd
    import numpy as np
    import pyodbc
    from datetime import date
    
    #variable instantiation
    SQL = ""
    today = date.today().strftime("%Y%d%m")
    
    #5-2a: pull the ODS_Table from ETL_Directory
    print('Executing 5-2a')
    #ods_table = ETL_Directory['ODS_Table'][0]
    ods_table = 'ODS_test_CLAIMS'
    
    #5-2b: iterate through each column, find the max length, and make part of the SQL string
    print('Executing 5-2b')
    for col in Inbound_File_Data.columns:
        SQL = SQL + col + " VARCHAR(" + str(max(Inbound_File_Data[col].astype(str).str.len())) + "), "
    
    #5-2c: write out the SQL string for a CREATE TABLE statement, remove the extra 2 characters, ", ", at the end of the SQL string
    print('Executing 5-2c')
    #SQL = f"CREATE TABLE {ods_table} (" + SQL[:-2] + ")"

    SQL = ( f"CREATE TABLE {ods_table}_{today}} (" + SQL[:-2] + ")"
            + ") ON [PRIMARY];"
            + f"SET IDENTITY_INSERT {ods_table}_{today} ON;"
            + f"INSERT INTO {ods_table}_{today} ()"
           )
           
        
        SET IDENTITY_INSERT ODS_DEVOTED_QUALITY_new20221013133500 ON;
        INSERT INTO dbo.ODS_DEVOTED_QUALITY_new20221013133500 ()
        SELECT FROM dbo.ODS_DEVOTED_QUALITY
        SET IDENTITY_INSERT ODS_DEVOTED_QUALITY_new20221013133500 OFF;
        
        EXEC sp_rename @objname = 'ODS_DEVOTED_QUALITY'
            ,@newname = 'ODS_DEVOTED_QUALITY_20221013133500';
        
        EXEC sp_rename @objname = 'ODS_DEVOTED_QUALITY_new20221013133500'
            ,@newname = 'ODS_DEVOTED_QUALITY'           

        
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
    #cursor.execute(SQL)
    
    cursor.commit()
    cursor.close()
    conn.close()
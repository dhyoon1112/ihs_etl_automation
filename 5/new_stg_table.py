#5-1 New STG Table
def new_stg_table(ETL_Directory, Inbound_File_Data, credentials):

    print('Executing new_stg_table.py, 5-1')    
    import pandas as pd
    import numpy as np
    import pyodbc
    
    #variable instantiation
    SQL = ""
    
    #5-1a: pull the STG_Table from ETL_Directory
    print('Executing 5-1a')
    stg_table = ETL_Directory['STG_Table'][0]
    
    #5-1b: find the max lengths of each column in the Inbound_File_Data
    print('Executing 5-1b')
    max_lengths = [max(Inbound_File_Data[col].astype(str).str.len()) for col in Inbound_File_Data.columns]
    
    #5-1c: round up to the nearest 50th value in max_lengths
    print('Executing 5-1c')
    rounded_lengths = [50 if i <= 50 else 100 for i in max_lengths]       
    
    #5-1d: new_stg_table_lengths dataframe for column names and their rounded lengths
    print('Executing 5-1d')
    new_stg_table_lengths = pd.DataFrame(columns = Inbound_File_Data.columns)
    new_stg_table_lengths.loc[0] = rounded_lengths
    
    #5-1e: iterate through each column, retrieve its respective value in row 0, and make part of the SQL string    
    print('Executing 5-1e')
    for col in new_stg_table_lengths:
        SQL = SQL + col + " VARCHAR(" + new_stg_table_lengths.loc[0,col].astype(str) + "), "
    
    #5-1f: write out the SQL string for a DROP and CREATE TABLE statement, remove the extra 2 characters, ", ", at the end of the SQL string
    print('Executing 5-1f')
    SQL = "CREATE TABLE {} (".format(stg_table) + SQL[:-2] + ")"

    #5-1g: execute the SQL string in EDW_STG
    print('Executing 5-1g')
    
    #variable instantiation
    server = credentials[0]
    database = credentials[1]
    username = credentials[2]
    password = credentials[3]
    
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    #create STG table, will print SQL error results if there are any
    cursor.execute(SQL)

    cursor.commit()
    cursor.close()
    conn.close()
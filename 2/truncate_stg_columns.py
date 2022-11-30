#2-1: Columns found in the respective STG Table
def truncate_stg_columns(ETL_Directory, credentials):

    print("Executing stg_columns.py, 2-1")
    import pyodbc
    import pandas as pd
    
    #variable instantiation
    server = credentials[0]
    database = credentials[1]
    username = credentials[2]
    password = credentials[3]    
    
    stg_table = str(ETL_Directory.loc[0, 'STG_Table']).replace("'","").strip()

    #2-1a: Connect to EDW_STG Database
    print('Executing 2-1a')    
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    #2-1b: Truncate and Select column names from related STG table, except RecordId column (identity column)
    print('Executing 2-1b')
    SQL = f"TRUNCATE TABLE {stg_table}; SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{stg_table}' AND COLUMN_NAME <> 'RecordId' ORDER BY ORDINAL_POSITION"
    col_names = cursor.execute(SQL).fetchall()
        
    #2-1c: Store column names to dataframe
    print('Executing 2-1c')        
    data = pd.DataFrame(columns=[i[0] for i in col_names])

    cursor.commit()
    cursor.close()
    conn.close()
    
    return data
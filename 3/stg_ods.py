#3-1 ODS Stored Procedure
def stg_ods(ETL_Directory, credentials):

    import pandas as pd
    import pyodbc
    print('Executing stg_ods.py, 3-1')
    
    #3-1a: pull the STG_ODS_Storedprocedure from the ETL_Directory
    print('Executing 3-1a')
    stored_procedure = ETL_Directory['STG_ODS_Storedprocedure'][0]
    
    #3-1b: exit script if no ODS stored procedure is found
    if len(stored_procedure) == 0:
        print('Executing 3-1b')
        return

    #3-1c: Execute the stored proc in Stored_Procedure    
    #variable instantiation
    server = credentials[0]
    database = credentials[1]
    username = credentials[2]
    password = credentials[3]
    
    '''
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()
        cursor.fast_executemany = True
    
        #Insert STG_Columns to STG Table
        print(f"Executing 3-1c, 'EXEC {stored_procedure}'")
        SQL = f"EXEC {stored_procedure}"
        cursor.execute(SQL)
    
        cursor.commit()
        cursor.close()
        conn.close()
    
    except pyodbc.Error as ex:
        print("Error: ")
        sqlstate = ex.args[1]
        sqlstate = sqlstate.split(".")

        cursor.commit()
        cursor.close()
        conn.close()
        
        return sqlstate[-3]
    '''

    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    #Insert STG_Columns to STG Table
    print(f"Executing 3-1c, 'EXEC {stored_procedure}'")
    SQL = f"SET NOCOUNT ON EXEC {stored_procedure}"
    cursor.execute(SQL).fetchall()
    
    cursor.commit()
    cursor.close()
    conn.close()
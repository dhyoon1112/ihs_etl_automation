#4-1 RPT Stored Procedure
def ods_rpt(ETL_Directory):

    import pandas as pd
    import pyodbc
    print('Executing ods_rpt.py, 4-1')    
    
    #4-1a: pull the STG_ODS_Storedprocedure from the ETL_Directory
    print('Executing 4-1a')
    stored_procedure = ETL_Directory['ODS_RPT_Storedprocedure'][0]

    #4-1b: Execute the stored proc in Stored_Procedure    
    #variable instantiation
    server = credentials[0]
    database = credentials[1]
    username = credentials[2]
    password = credentials[3]
    
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.fast_executemany = True

    #Insert STG_Columns to STG Table
    print(f"Executing 4-1b, 'EXEC {stored_procedure}'")
    SQL = f"EXEC {stored_procedure}"
    results = cursor.execute(SQL).fetchall()
    print('Results:' + str(results))

    cursor.commit()
    cursor.close()
    conn.close()
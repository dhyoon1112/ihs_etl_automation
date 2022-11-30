#1-1: ETL Definition to pull configuration data about each ETL
def error(carrier, domain, errortype, error, credentials):

    import pyodbc
    import pandas as pd
    print('Executing error.py, 7-2')
    print(f"Error for {carrier} {domain}")    
    
    #variable instantiation
    server = credentials[0]
    database = credentials[1]
    username = credentials[2]
    password = credentials[3]

    #1-1a: establish connection to DataAssets.dbo.ETL_Directory
    print('Executing 7-2a')
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    #1-1b: Declare SQL string, converting Lastrun_Date column to varchar to prevent incorrect date conversion in python
    print('Executing 7-2b')
    errortype = str(errortype).replace("'","")
    error = str(error).replace("'","")
    SQL = f"EXEC EDW_ODS.dbo.LogError @PackageName = 'Python ETL Automation', @Datasource = '{carrier}', @Domain = '{domain}', @ErrorCode = '{errortype}', @ErrorDescription = '{error}'"
    cursor.execute(SQL)
    
    cursor.commit()
    cursor.close()
    conn.close()
    
    return
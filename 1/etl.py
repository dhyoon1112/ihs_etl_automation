#1-1: ETL Definition to pull configuration data about each ETL
def etl(carrier, domain, credentials):

    import pyodbc
    import pandas as pd
    print('Executing etl.py, 1-1')
    
    #variable instantiation
    server = credentials[0]
    database = credentials[1]
    username = credentials[2]
    password = credentials[3]

    #1-1a: establish connection to DataAssets.dbo.ETL_Directory
    print('Executing 1-1a')
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    #1-1b: Declare SQL string, converting Lastrun_Date column to varchar to prevent incorrect date conversion in python
    print('Executing 1-1b')
    SQL = '''SELECT 
           ID
          ,Domain
          ,Carrier
          ,FTP_Filepath
          ,STG_Table
          ,Preprocess_Script_Filepath
          ,ODS_Table
          ,STG_ODS_Storedprocedure
          ,RPT_Table
          ,FIL_Storedprocedure
          ,ODS_RPT_Storedprocedure
          ,Active
          ,CONVERT(VARCHAR(20),Lastrun_Date) Lastrun_Date 
          FROM ETL_Directory
          '''

    #1-1c: execute query to select all records from ETL_Directory
    print('Executing 1-1c')
    ETL_Directory = cursor.execute(SQL).fetchall()

    #1-1d: Convert the pyodbc object to a list for each row in value
    print('Executing 1-1d')
    ETL_Directory = [str(i).split(",") for i in ETL_Directory]

    #1-1e: Convert the pyodbc object to a dataframe and assign column names
    print('Executing 1-1e')
    ETL_Directory = pd.DataFrame(ETL_Directory)
    ETL_Directory.columns = ['ID','Domain','Carrier','FTP_Filepath','STG_Table','Preprocess_Script_Filepath','ODS_Table','STG_ODS_Storedprocedure','RPT_Table','FIL_Storedprocedure','ODS_RPT_Storedprocedure','Active','Lastrun_Date']
    
    #1-1f: Clean the dataframe. Replace extra quotations, parentheses, and blank spaces
    print('Executing 1-1f')
    ETL_Directory = ETL_Directory.applymap(lambda x: x.replace("'", "").replace(")","").strip())

    #1-1g: Find the record in the ETL Directory unique to the Carrier and Domain
    print('Executing 1-1g')
    ETL_Directory = ETL_Directory.loc[(ETL_Directory["Domain"] == domain) & (ETL_Directory["Carrier"] == carrier)]
    
    #1-1h: Reset the index in ETL_Directory, to set the first row ID back to 0
    print('Executing 1-1h')
    ETL_Directory = ETL_Directory.reset_index()

    cursor.commit()
    cursor.close()
    conn.close()
    
    return ETL_Directory

#7-1: Contract ID Identifier
def contractid_identifier(Inbound_Files, credentials):

    print('Executing contractid_identifier, 7-1')
    import pandas as pd
    import numpy as np
    import pyodbc
    import os
    
    #variable instantiation
    filenames = ''
    
    #7-1a: concatenate all filenames to a formatted string for SQL execution
    print('Executing 7-1a')
    for i in Inbound_Files['files']:
        filenames = filenames + os.path.basename(i) + "','"
       
       
    #7-1b: execute the SQL string in EDW_ODS
    
    #variable instantiation
    server = credentials[0]
    database = credentials[1]
    username = credentials[2]
    password = credentials[3]
    
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    SQL = ( f"SELECT f.AssetId, fp.ContractId, fp.NLP_ContractId FROM DataAssets.dbo.FileAsset f INNER JOIN DataAssets.ai.FileAsset_Prediction fp ON f.AssetId = fp.AssetId WHERE f.Filename in ('{filenames[:-2]})" )
    print("Executing 7-1b")
    
    #create ODS table, will print SQL error results if there are any
    results = cursor.execute(SQL).fetchall()
    print(results)
    
    cursor.commit()
    cursor.close()
    conn.close()
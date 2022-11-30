#2-3 and 2-4: Insert records into STG. Two functions, 2-3 calls upon 2-4

#2-3: Insert inbound_file_data mapped with stg_columns into the respective stg_table
def stg_insertion(Inbound_File_Data, STG_Columns, ETL_Directory, inbound_filepaths, credentials):

    print('Executing stg_insertion() in stg_insert.py, 2-3')
    import pandas as pd
    import os

    #variable instantiation
    New_Columns = []
    values = ''
    
    #2-3a: pull the Staging table from the single record left in ETL_Directory
    print('Executing 2-3a')
    stg_table = ETL_Directory['STG_Table'][0]

    #2-3b: map columns from Inbound_File_Data to STG_Columns to match the STG Table
    print('Executing 2-3b')
    for col in Inbound_File_Data.columns:

        #Update values in STG_Columns
        if col in STG_Columns.columns:
            STG_Columns[col] = Inbound_File_Data[col]
    
        #Record columns that are not available in the STG table
        else:
            New_Columns.append(col)
                      
                      
    #2-3c: Exit definition with 1 if there are any rejected columns. Non-matching file
    if len(New_Columns) > 0:
        print(f"Executing 2-3c: Mismatching columns found: {New_Columns}")
        return 1

    #2-3d: update all NaN values to blank strings ('') for Innovista's columns
    print('Executing 2-3d')
    for col in STG_Columns.columns:
    
        if col in ['ContractId','DataSource','RecordId','RecordStaging_dt','ReportPeriod','NetworkName']:
            STG_Columns[col] = 'nan'
            
            
        elif col in ['RecordExtract_dt']:
            STG_Columns[col] = 'GETDATE()'
            
        
        elif col in ['SourceFilename']:
            STG_Columns[col] = os.path.basename(inbound_filepaths)
        
            
    #2-3e: create one long string of all values, formatted for the query insertion into the STG table
    print('Executing 2-3e')
    try:
        for i in range(len(STG_Columns)):

            #2-3e: strip each value and insert it into a list, removing extra quotations in the strings
            step = '2-3f'
            row = [str(j).strip().replace("'","").replace('"','') for j in STG_Columns.loc[i,].values]

            #2-3f: join each record to one string
            step = '2-3g'
            row = "','".join(row)
    
            #2-3g: add parenthesis and quotations for the query insertion step
            step = '2-3h'
            row = "('{}'),".format(row)
    
            #2-3h: add the formatted string to the master string which holds all records
            step = '2-3i'
            values = values + row
                
            #if the row count is at 1000, or at the last record, execute stg_query()
            if i%1000 == 0 or i == (len(STG_Columns)-1):
            
                #2-3i: remove the extra comma at the end of the values string
                step = '2-3j'
                values = values[:-1]
                
                #2-3j: prepare the stg_columns_string parameter for the query insertion. a single string of all columns headers
                step = '2-3k'
                stg_columns_string = ', '.join(STG_Columns.columns)
                
                #2-4: execute stg_query definition, stg_table is parameter passed, stg_columns is declared in 2c-3f, values is declared in 2c-3d, i is iterator cnt.
                step = '2-4'
                stg_query(stg_table, stg_columns_string, values, i, credentials)
                
                #truncate values variable
                values = ''
                                
            i += 1
        
    except:
        print('Error on step {} for row {} in STG_Columns (0 based numbering)'.format(step, i))
        raise Exception()
        
    #return 0 if there are no mismatching columns. relates to step 2-3c
    return 0
        

#2-4: definition to push data to STG 
def stg_query(stg_table, stg_columns_string, values, count, credentials):
    
    print('Executing stg_query() in stg_insert.py, 2-4')
    import pyodbc
    
    print('Executing 2-4a')
    #connect to EDW_STG Database and execute the query
    #variable instantiation
    server = credentials[0]
    database = credentials[1]
    username = credentials[2]
    password = credentials[3]
    
    print('Executing 2-4b')
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.fast_executemany = True

    #Insert STG_Columns to STG Table, replace any "NaN"s with "NULL"
    print('Executing 2-4, Inserting {} records into {}'.format(count,stg_table))
    SQL = f"INSERT INTO {stg_table} ({stg_columns_string}) VALUES {values}"
    SQL = SQL.replace("'nan'",'NULL').replace("'GETDATE()'", "GETDATE()")
    #print(SQL)
    cursor.execute(SQL)

    cursor.commit()
    cursor.close()
    conn.close()
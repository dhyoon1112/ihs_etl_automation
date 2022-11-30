#1-2: List of inbound files and their preprocessor
def inbound_files(ETL_Directory, carrier, domain):

    import pandas as pd
    import numpy as np
    import os
    import sys
    print('Executing inbound_files.py, 1-2')

    #variable instantiation
    inbound_files = pd.DataFrame()
    root = '\\\\172.24.16.35\\da'
          
          
    #1-2a: declare the carrier and domain, removing extra quotations and spaces
    print('Executing 1-2a')
    ods_table = str(ETL_Directory.loc[0, 'ODS_Table']).replace("'","").strip()

    #1-2b: declare Preprocessor fullpath, removing extra quotations and spaces
    print('Executing 1-2b')
    preprocessor = str(ETL_Directory.loc[0, 'Preprocess_Script_Filepath']).replace("'","").strip()
    
    #1-2c: list of all files in directory using list comprehension, with fullpath listed
    print('Executing 1-2c')
    files = [(root + '\\' + carrier + '\\' + domain + '\\' + 'Inbound\\') + i for i in os.listdir(root + '\\' + carrier + '\\' + domain + '\\' + 'Inbound\\')]
    
    #1-2d: dataFrame of inbound files and their related configurations, values are repeated for # of inbound files per carrier_domain
    print('Executing 1-2d')
    files_config = pd.DataFrame({  'carrier_domain' :  np.repeat([carrier + "_" +  domain], len(files)),
                                   'preprocessor' : np.repeat([preprocessor], len(files)),
                                   'ods_table' : np.repeat([ods_table], len(files)),
                                   'files' : files })
    
    #1-2e: concatenate each files_config to the dataframe inbound_files
    print('Executing 1-2e')
    inbound_files = pd.concat([inbound_files,files_config])
    
    #1-2f: exit script if no inbound files are found
    if len(inbound_files) == 0:
        print("No Inbound files found")
        raise SystemExit()

    return inbound_files
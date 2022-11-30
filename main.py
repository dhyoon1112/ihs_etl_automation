import pandas as pd
import sys
import os
import os.path
import pyodbc
import subprocess
import numpy as np
import csv
import argparse
from datetime import datetime

#retrieve credentials
import credentials
dataassets_credentials = credentials.credentials('DataAssets')
stg_credentials = credentials.credentials('EDW_STG')
ods_credentials = credentials.credentials('EDW_ODS')

#variable instantiation
rootpath = '//771483-mssql2/da/Scripts/Python ETL Automation'
today = datetime.now().isoformat(timespec='minutes')  
New_Columns_flags = []

#main script execution
print('Executing main.py')
try:
    if __name__ == "__main__":
        parser = argparse.ArgumentParser()
        parser.add_argument("-carrier") #Carrier
        parser.add_argument("-domain") #Domain
        args = parser.parse_args()
        carrier = args.carrier
        domain = args.domain
    
        print(f"---------------------{today}---------------------")
        print("Executing main.py")
        
        #-----------------------------------------------1. Setup Phase-----------------------------------------------------#
        #Retrieve list of ETL flows, their respective Inbound files, and Preprocess them. Import definitions from their respective files as modules to call upon.
        sys.path.append(rootpath + '/1')
        import etl
        import inbound_files
        import preprocess
        #import ftp
        
        #1-1: Retrieve the Directory of ETL Flows
        ETL_Directory = etl.etl(carrier, domain, dataassets_credentials)
        
        #1-2: List of Inbound Files for each ETL Flow. If there are no files, exit the script
        Inbound_Files = inbound_files.inbound_files(ETL_Directory, carrier, domain)
        
        #1-3: Execute FTP script
        #ftp.ftp(ETL_Directory)
        
        #1-4: Preprocess Files
        preprocess.preprocess(Inbound_Files)
        
        
        
        #-----------------------------------------------2. Staging Phase-----------------------------------------------------#
        #Retrieve STG Columns, map Inbound file, and Insert into the respective STG table
        sys.path.append(rootpath + '/2')
        import truncate_stg_columns
        import inbound_file_data
        import stg_insertion
        import archive_inbound_file
        
        #2-1: Truncate STG Table and retrive STG Table Columns
        STG_Columns = truncate_stg_columns.truncate_stg_columns(ETL_Directory, stg_credentials)
        
        #Iterate through each inbound file and insert into STG if available
        for inbound_filepaths in Inbound_Files['files']:
        
            #2-2: Inbound File Data, automatically identifies delimiter character 
            Inbound_File_Data = inbound_file_data.inbound_file_data(inbound_filepaths)
        
            #2-3 and 2-4: Insert data into STG. Variable instantiation executes the stg_insertion step. Append new columns to the list, New_Columns
            STG_Insertion_Results = stg_insertion.stg_insertion(Inbound_File_Data, STG_Columns, ETL_Directory, inbound_filepaths, stg_credentials)
            New_Columns_flags.append(STG_Insertion_Results)
            
            #2-5: Archive inbound file
            archive_inbound_file.archive_inbound_file(inbound_filepaths,carrier,domain)
        
    
    
        #-----------------------------------------------5. New ETL Phase-----------------------------------------------------#    
        #Create a new STG table, ODS table and new directories if not already available
        sys.path.append(rootpath + '/5')
        import new_stg_table
        import new_ods_table
        import new_directories
        
        #If the sum of flags for new(nonmatching) columns > 0 from STG_Insertion, and the ETL flow hasn't been run before, skip to step 5
        if sum(New_Columns_flags) > 0 and ETL_Directory['Lastrun_Date'][0] == 'None':
            
            #5-1: Create new STG table
            new_stg_table.new_stg_table(ETL_Directory, Inbound_File_Data, stg_credentials)
            
            #5-2: Create new ODS table
            new_ods_table.new_ods_table(ETL_Directory, Inbound_File_Data, ods_credentials)
            
            #5-3: Create new folderpaths: @Carrier\@Domain\Inbound, @Carrier\@Domain\Archive and @Carrier\@Domain\Resource
            new_directories.new_directories(carrier,domain)
            
        
        
        #-----------------------------------------------3. ODS Phase-----------------------------------------------------#    
        #Push STG data to ODS
        sys.path.append(rootpath + '/3')
        import stg_ods
        
        #3-1: ODS Stored Procedure
        stg_ods_results = stg_ods.stg_ods(ETL_Directory, ods_credentials)
        print(stg_ods_results)
        
        
        '''
        #-----------------------------------------------4. RPT Phase-----------------------------------------------------#    
        #Push ODS data to RPT
        sys.path.append(rootpath + '/4')
        import ods_rpt
        
        #4-1: RPT Stored Procedure
        results = ods_rpt.ods_rpt(ETL_Directory, rpt_credentials)
        print(results)
        
        
        
        
        #-----------------------------------------------6. Restructure ETL Phase-----------------------------------------------------#
        #If the sum of flags for new(nonmatching) columns > 0 from STG_Insertion, and the ETL flow hasn't been run before, skip to step 5
        #sys.path.append(rootpath + '/6')
        
        '''
        
        
        #-----------------------------------------------7. Contract ID (Asset ID) Machine Learning Assignment-----------------------------------------------------#
        #If the sum of flags for new(nonmatching) columns > 0 from STG_Insertion, and the ETL flow hasn't been run before, skip to step 5
        sys.path.append(rootpath + '/7')
        import contractid_identifier
        
        contractid_identifier.contractid_identifier(Inbound_Files, dataassets_credentials)
    
except Exception as e:
    sys.path.append(rootpath + '/7')
    import error
    error.error(carrier, domain, TypeError, e, ods_credentials)
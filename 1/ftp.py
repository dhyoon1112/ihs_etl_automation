#1-3: Preprocess each Inbound File
def ftp(ETL_Directory): 

    import pandas as pd
    import subprocess
    print('Executing ftp.py, 1-3')    

    #1-3a: preprocess each file with its respective preprocessor
    print('Executing 1-3a')
    try:
        
        #1-3b: preprocessor file, replace \\ with \
        step = '1-3b'
        ftp_script = str(ETL_Directory.loc[i, 'FTP_Filepath']).replace("'","").replace("\\\\","\\").strip()
        
        if ftp_script == 'None':
            print('No ftp script found')
            return
        
        #1-3d: preprocess string + inbound file
        step = '1-3d'
        ftp_script_execution = f'"C:\Program Files (x86)\WinSCP\WinSCP.com" -script={ftp_script}'
        
        #1-3e: execute preprocessor
        step = '1-3e'
        print("Executing 1-3e: Running Shell command: " + ftp_script_execution)
        subprocess.call(ftp_script_execution, shell=True)
        
    except:
        print('Error in ftp.py execution')
        raise Exception()
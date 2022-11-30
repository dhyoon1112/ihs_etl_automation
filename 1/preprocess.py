#1-4: Preprocess each Inbound File
def preprocess(Inbound_Files): 

    import pandas as pd
    import subprocess
    print('Executing preprocess.py, 1-4')    

    #1-4a: preprocess each file with its respective preprocessor
    print('Executing 1-4a')
    try:
        
        for i in range(len(Inbound_Files.index)):

            #1-4b: preprocessor file, replace \\ with \
            step = '1-4b'
            preprocessor = str(Inbound_Files.loc[i, 'preprocessor']).replace("'","").replace("\\\\","\\").strip()
            
            if preprocessor == 'None':
                print('No preprocessor script found')
                return
        
            #1-4c: inbound file
            step = '1-4c'
            inboundfile = str(Inbound_Files.loc[i,'files'])
        
            #1-4d: preprocess string + inbound file
            step = '1-4d'
            preprocessor_inboundfile = 'python ' + '"' + str(preprocessor) + '" -i "' + inboundfile + '"'
    
            #1-4e: execute preprocessor
            step = '1-4e'
            print("Executing 1-4e: Running Shell command: " + preprocessor_inboundfile)
            subprocess.call(preprocessor_inboundfile, shell=True)
        
    except:
        print('Error on step {} for row {} in Inbound_Files (0 based numbering)'.format(step, i))
        raise Exception()
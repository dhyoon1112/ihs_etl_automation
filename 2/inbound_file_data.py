#2-2: Retrieve the data for the respective inbound_file
def inbound_file_data(inbound_file):
    
    print("Executing inbound_file_data.py, 2-2")    
    import csv
    import pandas as pd
    
    #2-2a: open and close inbound file
    print('Executing 2-2a')
    with open(inbound_file, 'r') as csvfile:
        
        #2-2b: identify the delimiter of the file
        print('Executing 2-2b')
        delimiter = str(csv.Sniffer().sniff(csvfile.read()).delimiter)
        
        #2-2c: read the file with said delimiter
        print('Executing 2-2c')
        #file = pd.read_csv(inbound_file, sep = delimiter, keep_default_na=True, quotechar = "'")
        file = pd.read_csv(inbound_file, sep = delimiter, keep_default_na=True, quotechar = '"')
        
        #file = file.applymap(lambda x: x.replace("'", "\\'").replace('\"','\\"'))
        #print(file)

    return file
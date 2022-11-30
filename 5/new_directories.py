#5-3 New directories
def new_directories(carrier,domain):

    print('Executing new_directories.py, 5-3')
    import os
   
    #5-3a: create new inbound folder if not already available
    print('Executing 5-3a')
    rootpath = f"//771483-mssql2/da/{carrier}/{domain}/"
    if os.path.exists(rootpath) == False:
        os.mkdir(rootpath)
        
    #5-3b: create new resource folder if not already available
    print('Executing 5-3b')
    resourcepath = f"//771483-mssql2/da/{carrier}/Resource/"
    if os.path.exists(resourcepath) == False:
        os.mkdir(resourcepath)        
   
    #5-3c: create new inbound folder if not already available
    print('Executing 5-3c')
    inboundpath = rootpath + "Inbound"
    if os.path.exists(inboundpath) == False:
        os.mkdir(inboundpath)
    
    #5-3d: create new archive folder if not already available
    print('Executing 5-3d')    
    archivepath = rootpath + "Archive"        
    if os.path.exists(archivepath) == False:
        os.mkdir(archivepath)
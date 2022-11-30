#2-5 New directories
def archive_inbound_file(inbound_filepath,carrier,domain):

    print('Executing archive_inbound_file.py, 2-5')
    import shutil

    #2-5a: declare inbound and archive directories
    print('Executing 2-5a')
    inbound_directory = inbound_filepath
    archive_directory = f"//771483-mssql2/da/{carrier}/{domain}/Archive"
    
    #2-5b: move inbound file to archive directory
    print('Executing 2-5b')
    shutil.move(inbound_directory, archive_directory)
        
    '''    
    from pathlib import Path
    Path("path/to/current/file.foo").rename("path/to/new/destination/for/file.foo")
    '''
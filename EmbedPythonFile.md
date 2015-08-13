# How to work with Files #

This page only describes the python microservices, see EmbedPythonUsage for examples on how to call these microservice with the **irule** command.



## Get file information ##

```
def msiGetInfos(path, resStr, rei):
    
    f = iRodsOpen(rei.getRsComm(), path.parseForStr(), 'w')
    f.write("\/"*25)
    
    res = (f.getCollName(),
           f.getDataName(),
           f.getDescInx(),
           f.getPosition(),
           f.getReplNumber(),
           f.getResourceName(),
           f.getSize() )  # size = 0 because size is updated when you close the file
    
    f.close()
    
    f.delete()
    
    
    fillStrInMsParam(resStr, str(res))
```


## Manage metadatas ##

```
def msiTestMetadata(path, resStr, rei):
    res = '' 
    
    # 1st version
    addFileUserMetadata(rei.getRsComm(), path.parseForStr(), "units", "12", "cm")
    addFileUserMetadata(rei.getRsComm(), path.parseForStr(), "author", "rods")
    res += str(getFileUserMetadata(rei.getRsComm(), path.parseForStr()))
    rmFileUserMetadata(rei.getRsComm(), path.parseForStr(), "author", "rods")
    res += str(getFileUserMetadata(rei.getRsComm(), path.parseForStr()))  

    # 2nd version    
    f = iRodsOpen(rei.getRsComm(), path.parseForStr(), 'w')
     
    if f:             
        # Add some metadata
        f.addUserMetadata("units", "12", "cm")
        f.addUserMetadata("author", "rods")
        res += str(f.getUserMetadata())
        f.rmUserMetadata("author", "rods")
        res += str(f.getUserMetadata()) 
       
        f.close()
        f.delete()
    
    else:
        res += "File", path.parseForStr(), "does not exist"
        
    fillStrInMsParam(resStr, str(res))
```


## Read/Write/Append ##

```
def msiTestReadWriteAppend(inPath, resStr, rei):  
    
    path = inPath.parseForStr()
    res = ''
    
    # Write a string in a file
    f = iRodsOpen(rei.getRsComm(), path, 'w')
    f.write("This is a test") 
    f.close()
    
    # Read the file
    f = iRodsOpen(rei.getRsComm(), path, 'r')
    res +=  f.read() + '\n'
    f.close()    
    
    # Read the file (give the buffer size)
    f = iRodsOpen(rei.getRsComm(), path, 'r')
    res += f.read(5) + '\n'
    res += f.read() + '\n'
    f.close()    
    
    # Append to the file
    f = iRodsOpen(rei.getRsComm(), path, 'a')
    f.write("\nThis is still the test")
    f.close()

    # Read the file again
    f = iRodsOpen(rei.getRsComm(), path, 'r')
    res += f.read()
    f.close()    
    
    fillStrInMsParam(resStr, str(res))
```

## Seek ##

```
def msiTestSeek(inPath, resStr, rei):  
    
    path = inPath.parseForStr()
    res = ''    
    
    # Write a string in a file
    f = iRodsOpen(rei.getRsComm(), path, 'w')
    f.write("-" * 100)
    f.close()

    # Read the file from several positions
    f = iRodsOpen(rei.getRsComm(), path, 'r')
    res +=  "Size :%d\n" % f.getSize()
    f.seek(50, os.SEEK_SET) # middle
    res += f.read() + '\n'
        
    f.seek(0) # begining    
    res += f.read() + '\n'
    f.seek(f.getSize(), os.SEEK_END) # begining (from the end)
    res += f.read()    + '\n' 
    f.close()
    
    # Modify the file 
    f = iRodsOpen(rei.getRsComm(), path, 'a')
    f.seek(-60, os.SEEK_CUR) # middle
    res += "Begin position of modify : %d\n" % f.getPosition()
    f.write("+" * 20)
    res += "End position of modify : %d\n" % f.getPosition()   
    f.close()   
    
    # Read the modified file 
    f = iRodsOpen(rei.getRsComm(), path, 'r')
    res += f.read()   + '\n'  
    f.close()
    
    f.delete()    
```


## Access to replicate files ##

```
def msiTestReplication(inPath, resc1, resc2, resStr, rei):
    
    path = inPath.parseForStr()
    
    res = ''
    
    
    # If path exists on resc2, it will modify the version on resc2 and not create
    # a new one on resc1. This is the choice of irods team.
    f = iRodsOpen(rei.getRsComm(), path, 'w', resc1.parseForStr())
    f.write("=="*15)
    f.close()
    
    res += "First read, both files are equal\n"
    
    f = iRodsOpen(rei.getRsComm(), path, 'r', resc1.parseForStr())
    res += "  Path: %s/%s\n" % (f.getCollName(), f.getDataName())
    res += "  Resource Name :" + f.getResourceName() + '\n'
    res += "  Repl Number : %d\n" % f.getReplNumber()
    res += "  Size :" + str(f.getSize()) + '\n'
    res += "  Content :" +  f.read() + '\n\n'
    f.replicate(resc2.parseForStr())
    f.close()
    
    f = iRodsOpen(rei.getRsComm(), path, 'r', resc2.parseForStr())
    res += "  Path: %s/%s\n" % (f.getCollName(), f.getDataName())
    res += "  Resource Name :" + f.getResourceName() + '\n'
    res += "  Repl Number : %d\n" % f.getReplNumber()
    res += "  Size :" + str(f.getSize()) + '\n'
    res += "  Content :" + f.read() + '\n\n'
    f.close()
    
    
    res += "Second read, first file is modified\n"
    
    f = iRodsOpen(rei.getRsComm(), path, 'a', resc1.parseForStr())
    f.write("++"*15)
    f.close()
    
    f = iRodsOpen(rei.getRsComm(), path, 'r', resc1.parseForStr())
    res += "  Path: %s/%s\n" % (f.getCollName(), f.getDataName())
    res += "  Resource Name :" + f.getResourceName() + '\n'
    res += "  Repl Number : %d\n" % f.getReplNumber()
    res += "  Size :" + str(f.getSize()) + '\n' 
    res += "  Content :" + f.read() + '\n\n'
    f.close()
    
    f = iRodsOpen(rei.getRsComm(), path, 'r', resc2.parseForStr())
    res += "  Path: %s/%s\n" % (f.getCollName(), f.getDataName())
    res += "  Resource Name :" + f.getResourceName() + '\n'
    res += "  Repl Number : %d\n" % f.getReplNumber()
    res += "  Size :" + str(f.getSize()) + '\n'
    res += "  Content :" + f.read() + '\n\n'
    f.close()
    
    res += "Third read, synchronize the versions\n"
    
    f.update()
    
    f = iRodsOpen(rei.getRsComm(), path, 'r', resc1.parseForStr())
    res += "  Path: %s/%s\n" % (f.getCollName(), f.getDataName())
    res += "  Resource Name :" + f.getResourceName() + '\n'
    res += "  Repl Number : %d\n" % f.getReplNumber()
    res += "  Size :" + str(f.getSize()) + '\n'
    res += "  Content :" + f.read() + '\n\n'
    f.close()
    
    f = iRodsOpen(rei.getRsComm(), path, 'r', resc2.parseForStr())
    res += "  Path: %s/%s\n" % (f.getCollName(), f.getDataName())
    res += "  Resource Name :" + f.getResourceName() + '\n'
    res += "  Repl Number : %d\n" % f.getReplNumber()
    res += "  Size :" + str(f.getSize()) + '\n'
    res += "  Content :" + f.read() + '\n\n'
    f.close()
    
    res += "Get the replicas\n"
    
    f = iRodsOpen(rei.getRsComm(), path, 'r', resc1.parseForStr())
    
    for fi in f.getReplications():
        res += "  Path: %s/%s\n" % (fi.getCollName(), fi.getDataName())
        res += "  Resource Name :" + fi.getResourceName() + '\n'
        res += "  Repl Number : %d\n" % fi.getReplNumber()
        res += "  Size :" + str(fi.getSize()) + '\n'
        res += "  Content :" + fi.read() + '\n\n'
        
        fi.close()
        fi.delete()    
        
    f.close()
    
    
    fillStrInMsParam(resStr, str(res))
```
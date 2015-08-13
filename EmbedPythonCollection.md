# How to work with Collections #

This page only describes the python microservices, see EmbedPythonUsage for examples on how to call these microservice with the **irule** command.



## Manage collections and files in collections ##

```
def msiTestCollection(rescName1, rescName2, resStr, rei):  
        
    resc1 = rescName1.parseForStr()
    resc2 = rescName2.parseForStr()
    
    res = ''
    
    # Open the current working directory
    c = irodsCollection(rei.getRsComm())
    
    res += c.getCollName() + '\n'

    c.createCollection("testCollection")
    c.openCollection("testCollection")
    
    res += c.getCollName() + '\n'
    
    f = c.create("testCollection.txt")
    nb_bytes_written = f.write("This is a test")
    f.close()
    # REPLICATE THE FILE AFTER CLOSING IT (BECAUSE MODE IS 'w')
    f.replicate(resc2)    

    f = c.create("testCollection2.txt", resc2)
    nb_bytes_written = f.write("This is another test")
    f.close()
    
    res += "Number of data objects :%d\n %s\n" % (c.getLenObjects(), str(c.getObjects()))
    
    
    for dataObj in c.getObjects():
        data_name = dataObj[0]
        resc_name = dataObj[1]
        
        res += data_name + ' ' + resc_name + '\n'
        
        f = c.open(data_name, "r", resc_name)
        
        res += "  Path:" +  f.getCollName() + '/' + f.getDataName() + '\n'
        res += "  Resource Name :" + f.getResourceName() + '\n'
        res += "  Repl Number :" + str(f.getReplNumber()) + '\n'
        res += "  Size :" + str(f.getSize()) + '\n'
        res += "  Content :" + f.read() + '\n\n'        
        
        c.delete(data_name, resc_name)
        
        
    c.upCollection()
    
    res += c.getCollName() + '\n'
    res += "Number of subcollections :%d\n %s\n" % (c.getLenSubCollections(), str(c.getSubCollections()))
    
    
    res += "After deletion\n"
    c.deleteCollection("subCollection")
    res += "Number of subcollections :%d\n %s\n" % (c.getLenSubCollections(), str(c.getSubCollections()))
           
           
    fillStrInMsParam(resStr, str(res))
```


## Manage metadatas ##


```
def msiTestMetadata(path, resStr, rei):    
    c = irodsCollection(rei.getRsComm(), path.parseForStr())
 
    res = ''

    # 1st version 
    addCollUserMetadata(rei.getRsComm(), path.parseForStr(), "units", "12", "cm")
    addCollUserMetadata(rei.getRsComm(), path.parseForStr(), "author", "jerome")
    res += str(getCollUserMetadata(rei.getRsComm(), path.parseForStr()))
    rmCollUserMetadata(rei.getRsComm(), path.parseForStr(), "author", "jerome")
    res += str(getCollUserMetadata(rei.getRsComm(), path.parseForStr())   )        
    rmCollUserMetadata(rei.getRsComm(), path.parseForStr(), "units", "12", "cm")
    res += str(getCollUserMetadata(rei.getRsComm(), path.parseForStr()))
           
    # 2nd version
    if c:
        c.addUserMetadata("units", "12", "cm")
        c.addUserMetadata("author", "jerome")
        res += str(c.getUserMetadata())
        c.rmUserMetadata("author", "jerome")
        res += str(c.getUserMetadata())        
        c.rmUserMetadata("units", "12", "cm")    
        res += str(c.getUserMetadata())
        
        
    fillStrInMsParam(resStr, str(res))
```


# How to work with Resources #

This page only describes the python microservices, see EmbedPythonUsage for examples on how to call these microservice with the **irule** command.


## Get resource information ##

```
def msiGetResourceInfos(resourceName, resourceTupleStr, rei):   
    resc = getResource(rei.getRsComm(), resourceName.parseForStr())    

    if resc:
        rescTuple = (resc.getId(),
                     resc.getName(),
                     resc.getZone(),
                     resc.getTypeName(),
                     resc.getClassName(),
                     resc.getHost(),
                     resc.getPath(),
                     resc.getFreeSpace(),
                     resc.getFreeSpaceTs(),
                     resc.getInfo(),
                     resc.getComment())
    else:
        rescTuple = None
        
    fillStrInMsParam(resourceTupleStr, str(rescTuple))
```


## Create/Delete resources ##

```
def msiCreateResource(rescName, rescPath, resourceTupleStr, rei):
    

    resc = createResource(rei.getRsComm(), rescName.parseForStr(), "unix file system", 
                          "archive", "localhost", rescPath.parseForStr())
    
    if resc:
        rescTuple = (resc.getId(),
                     resc.getName(),
                     resc.getZone(),
                     resc.getTypeName())
    else:
        rescTuple = None  
        
    fillStrInMsParam(resourceTupleStr, str(rescTuple))
    
  
def msiDeleteResource(rescName, rei):    
    return deleteResource(rei.getRsComm(), rescName.parseForStr())
```


## Get a list of existing resources ##


```
def msiGetResources(resources, rei):
    fillStrInMsParam(resources, str([r.getName() for r in getResources(rei.getRsComm())]))
```

## Manage metadatas ##

```
def msiSetMetadata(rescName, name, value, units, rei):
    resc = getResource(rei.getRsComm(), rescName.parseForStr())
    
    if resc:        
        return resc.addUserMetadata(name.parseForStr(), 
                                    value.parseForStr(),
                                    units.parseForStr())
    else:
        return -1
    
    
def msiGetMetadata(rescName, metadatas, rei):
    resc = getResource(rei.getRsComm(), rescName.parseForStr())
    
    res = []
        
    if resc:        
        res = resc.getUserMetadata()
    
    fillStrInMsParam(metadatas, str(res))
    
    
def msiRmMetadata(rescName, name, value, units, rei):
    resc = getResource(rei.getRsComm(), rescName.parseForStr())
        
    if resc:        
        return resc.rmUserMetadata(name.parseForStr(), 
                                   value.parseForStr(),
                                   units.parseForStr())
    else:
        return -1 
```
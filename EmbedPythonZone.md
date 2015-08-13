# How to work with Zones #

This page only describes the python microservices, see EmbedPythonUsage for examples on how to call these microservice with the **irule** command.


## Get zone information ##

```
def msiGetZoneInfos(zoneName, zoneTupleStr, rei):   
    zone = getZone(rei.getRsComm(), zoneName.parseForStr())    

    if zone:
        zoneTuple = (zone.getId(),
                     zone.getTypeName(),
                     zone.getConn(),
                     zone.getCreateTs(),
                     zone.getComment())
    else:
        zoneTuple = None
        
    fillStrInMsParam(zoneTupleStr, str(zoneTuple))
```


## Create/Delete zones ##

```
def msiCreateZone(zoneName, comment, zoneTupleStr, rei):    
    zone = createZone(rei.getRsComm(), zoneName.parseForStr(), "remote", 
                      comment=comment.parseForStr())
    
    if zone:
        zoneTuple = (zone.getId(),
                     zone.getTypeName(),
                     zone.getConn(),
                     zone.getCreateTs(),
                     zone.getComment())
    else:
        zoneTuple = None  
        
    fillStrInMsParam(zoneTupleStr, str(zoneTuple))
  
  
def msiDeleteZone(zoneName, rei):    
    return deleteZone(rei.getRsComm(), zoneName.parseForStr())
```


## Get a list of existing zones ##


```
def msiGetZones(zones, rei):
    fillStrInMsParam(zones, str([z.getName() for z in getZones(rei.getRsComm())]))
```
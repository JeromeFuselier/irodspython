# How to work with Groups #

This page only describes the python microservices, see EmbedPythonUsage for examples on how to call these microservice with the **irule** command.


## Get group information ##

```
def msiGetGroupInfos(groupName, groupTupleStr, rei):   
    group = getGroup(rei.getRsComm(), groupName.parseForStr())    

    if group:
        groupTuple = (group.getId(),
                      group.getTypeName(),
                      group.getZone(),
                      group.getDN(),
                      group.getInfo(),
                      group.getComment(),
                      str([ u.getName() for u in group.getMembers() ]))
    else:
        groupTuple = None
        
    fillStrInMsParam(groupTupleStr, str(groupTuple))
```


## Create/Delete groups ##

```
def msiCreateGroup(groupName, groupInfos, rei):
    
    group = createGroup(rei.getRsComm(), groupName.parseForStr())  
    
    if group:
        groupTuple = (group.getId(),
                      group.getTypeName(),
                      group.getZone(),
                      group.getDN(),
                      group.getInfo(),
                      group.getComment(),
                      str([u.getName() for u in group.getMembers() ]))
    else:
        groupTuple = None  
        
    fillStrInMsParam(groupInfos, str(groupTuple))
    
    
def msiDeleteGroup(groupName, rei):
    return deleteGroup(rei.getRsComm(), groupName.parseForStr())
```


## Get a list of existing groups ##


```
def msiGetGroups(groups, rei):
    fillStrInMsParam(groups, str([g.getName() for g in getGroups(rei.getRsComm())]))
```

## Manage metadatas ##

```
def msiSetMetadata(groupName, name, value, units, rei):
    group = getGroup(rei.getRsComm(), groupName.parseForStr())
    
    if group:        
        return group.addUserMetadata(name.parseForStr(), 
                                     value.parseForStr(),
                                     units.parseForStr())
    else:
        return -1
    
    
def msiGetMetadata(groupName, metadatas, rei):
    group = getGroup(rei.getRsComm(), groupName.parseForStr())
    
    res = []
        
    if group:        
        res = group.getUserMetadata()
    
    fillStrInMsParam(metadatas, str(res))
    
    
def msiRmMetadata(groupName, name, value, units, rei):
    group = getGroup(rei.getRsComm(), groupName.parseForStr())
        
    if group:        
        return group.rmUserMetadata(name.parseForStr(), 
                                   value.parseForStr(),
                                   units.parseForStr())
    else:
        return -1 
```

## Add/Remove users ##

```
def msiAddUserGroup(groupName, userName, rei):
    group = getGroup(rei.getRsComm(), groupName.parseForStr())    
    
    if group:
        return group.addUser(userName.parseForStr())
    else:
        return -1
  
    
def msiRmUserGroup(groupName, userName, rei):
    group = getGroup(rei.getRsComm(), groupName.parseForStr())    
    
    if group:
        return group.rmUser(userName.parseForStr())
    else:
        return -1  
```
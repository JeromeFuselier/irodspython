# How to work with Users #

This page only describes the python microservices, see EmbedPythonUsage for examples on how to call these microservice with the **irule** command.


## Get user information ##

```
def msiGetUserInfos(userName, userTupleStr, rei):   
    user = getUser(rei.getRsComm(), userName.parseForStr())    

    if user:
        userTuple = (user.getId(),
                     user.getTypeName(),
                     user.getZone(),
                     user.getDN(),
                     user.getInfo(),
                     user.getComment(),
                     str([g.getName() for g in user.getGroups() ]))
    else:
        userTuple = None
        
    fillStrInMsParam(userTupleStr, str(userTuple))
```


## Create/Delete users ##

```
def msiCreateUser(userName, userType, userInfos, rei):
    user = createUser(rei.getRsComm(), 
                      userName.parseForStr(),
                      userType.parseForStr())  
    
    if user:
        userTuple = (user.getId(),
                     user.getTypeName(),
                     user.getZone(),
                     user.getDN(),
                     user.getInfo(),
                     user.getComment(),
                     str([g.getName() for g in user.getGroups() ]))
    else:
        userTuple = None  
    fillStrInMsParam(userInfos, str(userTuple))
    
    
def msiDeleteUser(userName, rei):
    return deleteUser(rei.getRsComm(), userName.parseForStr())
```


## Get a list of existing users ##


```
def msiGetUsers(users, rei):
    fillStrInMsParam(users, str([g.getName() for g in getUsers(rei.getRsComm())]))
```

## Manage metadatas ##

```
def msiSetMetadata(userName, name, value, units, rei):
    user = getUser(rei.getRsComm(), userName.parseForStr())
    
    if user:        
        return user.addUserMetadata(name.parseForStr(), 
                                    value.parseForStr(),
                                    units.parseForStr())
    else:
        return -1
    
    
def msiGetMetadata(userName, metadatas, rei):
    user = getUser(rei.getRsComm(), userName.parseForStr())
    
    res = []
        
    if user:        
        res = user.getUserMetadata()
    
    fillStrInMsParam(metadatas, str(res))
    
    
def msiRmMetadata(userName, name, value, units, rei):
    user = getUser(rei.getRsComm(), userName.parseForStr())
        
    if user:        
        return user.rmUserMetadata(name.parseForStr(), 
                                   value.parseForStr(),
                                   units.parseForStr())
    else:
        return -1 
```
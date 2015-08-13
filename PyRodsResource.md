# How to work with Resources #

## Get resource information ##


```
from irods import *

rescName = "demoResc"

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

resc = getResource(conn, rescName)    

# Get some information
print "Id:", resc.getId()
print "Name:", resc.getName()
print "Zone:", resc.getZone()
print "Type:", resc.getTypeName()
print "Class:", resc.getClassName()
print "Host:", resc.getHost()
print "Path:", resc.getPath()
print "Free Space:", resc.getFreeSpace()
print "Free Space TS:", resc.getFreeSpaceTs()
print "Info:", resc.getInfo()
print "Comment:", resc.getComment()
print "Create TS:", resc.getCreateTs()
print "Modify TS:", resc.getModifyTs()
    
# Modify some fields
#resc.setTypeName("unix file system")
#resc.setClassName("archive")
#resc.setHost("localhost.localdomain")
#resc.setPath("/home/cheshire/build/iRODS/Vault")
resc.setComment("Useful comment")
resc.setInfo("Useful info")
resc.setFreeSpace("free comment")  
    
conn.disconnect()

```


## Get a list of existing resources ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

# Get a list of iRodsResource instances
for resc in getResources(conn):
    print "Id:", resc.getId()
    print "Name:", resc.getName()
    
conn.disconnect()
```

## Create/delete resource ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

# Create a resource
resc = createResource(conn, "testResc", "unix file system", "archive", 
                      "localhost", " /home/jerome/testVault")
# Delete the resource
deleteResource(conn, "testResc") 

conn.disconnect()
```


## Manage metadatas ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

resc = getResource(conn, rescName)

# Get a list of metadatas, a metadata is a tuple (name, value, units)
resc.getUserMetadata()

# Add some metadatas, the unit field is optional        
resc.addUserMetadata("test1", "value1")
resc.addUserMetadata("test2", "value2", "units")

print resc.getUserMetadata()
    
# Remove the metadatas we added
resc.rmUserMetadata("test1", "value1")
resc.rmUserMetadata("test2", "value2", "units")
    
print resc.getUserMetadata()

conn.disconnect()
```
# How to work with Zones #


## Get zone information ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

zone = getZone(conn, myEnv.rodsZone)    

# Get Zone information
print "Id:", zone.getId()
print "Name:", zone.getName()
print "Type:", zone.getTypeName()
print "Conn String:", zone.getConn()
print "Comment:", zone.getComment()
print "Create TS:", zone.getCreateTs()
print "Modify TS:", zone.getModifyTs()

# You can modify some fields    
#zone.setConn("Conn Name")
#zone.setComment("Useful comment")
 
# Be careful with the modification of a local name as it will imply to
# modify .irodsEnv and core.irb
#zone.setName("tempZone2")
    
conn.disconnect()
```


## Get a list of existing zones ##


```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

# Get a list of iRodsZone instances
for zone in getZones(conn):
    print "Id:", zone.getId()
    print "Name:", zone.getName()

conn.disconnect()
```


## Create/Delete a zone ##


```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

# Create a zone, the comment parameter is optional, there's also a connstr optional param
zone = createZone(conn, "testZone", "remote", comment="test")
# Delete the zone
deleteZone(conn, "testZone") 

conn.disconnect()
```
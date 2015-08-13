# How to work with Groups #

## Get group information ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

# Get an iRodsGroup with its name
group = getGroup(conn, "public")   

# Get the fields
print "Id:", group.getId()
print "Name:", group.getName()
print "Type:", group.getTypeName()
print "Zone:", group.getZone()
print "Info:", group.getInfo()
print "Comment:", group.getComment()
print "Create TS:", group.getCreateTs()
print "Modify TS:", group.getModifyTs()

# You can modify some of the fields with an admin account
#group.setComment("Useful Comment")
#group.setInfo("Useful info")

# You can get the members of a group. You obtain iRodsUser instances    
for user in group.getMembers():
    print "Id:", user.getId()
    print "Name:", user.getName()
    print "Zone:", user.getZone()

conn.disconnect()
```


## Add/Remove groups ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

# Create a group testAPI
group = createGroup(conn, "testAPI")   
# Delete the group
print deleteGroup(conn, "testAPI")
    
conn.disconnect()
```


## Add/Remove a user in a group ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

group = createGroup(conn, "testAPI")    
# Add a user in the group            
group.addUser("cheshire")
# Remove the user from the group
group.rmUser("cheshire#cheshireZone")

deleteGroup(conn, "testAPI")

conn.disconnect()
```

## Manage metadatas ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

group = getGroup(conn, "public")   

# Get a list of metadatas, a metadata is a tuple (name, value, units)
print group.getUserMetadata()

# Add some metadatas, the unit field is optional
group.addUserMetadata("test1", "value1")
group.addUserMetadata("test2", "value2", "units")

print group.getUserMetadata()

# Remove the metadatas we added
group.rmUserMetadata("test1", "value1")
group.rmUserMetadata("test2", "value2", "units")

print group.getUserMetadata()

conn.disconnect()
```


## Get a list of existing users ##

```
status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

# Get a list of iRodsUser
for group in getGroups(conn):
    print group.getName()

conn.disconnect()
```
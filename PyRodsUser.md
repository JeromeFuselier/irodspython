# How to work with Users #

## Connection to a server ##

```
from irods import *

OBF_PWD_PATH = os.environ['HOME'] + "/.irods/.irodsA"

# Get the plain text password
status, password = obfGetPw()
# Get the encrypted password
status, obf_password = obfiGetPw(OBF_PWD_PATH)

# you can use obfiDecode(obf_password) or obfiEncode(password)
# to encode or decode a password

envVal = obfiGetEnvKey()
status, decoded_password = obfiDecode(obf_password, envVal)
print decoded_password
    
encoded_password = obfiEncode(password, envVal)
print encoded_password
    
status, decoded_password = obfiDecode(encoded_password, envVal)
print decoded_password

# Parse the .irodsEnv file
status, myEnv = getRodsEnv()

# Connection to a server with the default values
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)

# 3 different ways to log on the server
status = clientLogin(conn)
#status = clientLoginWithPassword(conn, password)
#status = clientLoginWithObfPassword(conn, obf_password)

# Do what you have to do
    
# Disconnect
conn.disconnect()
```


## Get user information ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)
    
# Get the information present in the iCAT
print getUserInfo(conn, "rods")

# Get an irodsUser object, the zone is optional
user = getUser(conn, myEnv.rodsUserName)
#user = getUser(conn, myEnv.rodsUserName, myEnv.rodsZone)
     
print "Id:", user.getId()
print "Name:", user.getName()
print "Type:", user.getTypeName()
print "Zone:", user.getZone()
print "Info:", user.getInfo()
print "Comment:", user.getComment()
print "Create TS:", user.getCreateTs()
print "Modify TS:", user.getModifyTs()

# You can modify some of the fields if you are admin
#user.setComment("Useful Comment")
#user.setInfo("Useful info")

# Be careful if you remove your user from rodsadmin you will have trouble to put it back
#user.setTypeName("rodsuser")
# Be careful with this one as changing the zone will change the authentication
#user.setZone("newZone")

# You can get the groups the user belongs to. You obtain irodsGroup instances
print "Member of :"
for g in user.getGroups():
    print "  -", g.getName()
  
conn.disconnect()
```


## Create/Delete users ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

# Create a user with the name and the group
user = createUser(conn, "test", "rodsuser") 

# Delete a user
deleteUser(conn, "test")

conn.disconnect()
```


## Get a list of existing users ##


```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

# Get a list of iRodsUser
for user in getUsers(conn):
    print "Id:", user.getId()
    print "Name:", user.getName()
    print "Type:", user.getTypeName()
    print "Zone:", user.getZone()
    print "Info:", user.getInfo()
    print "Comment:", user.getComment()
    print

conn.disconnect()
```

## Manage metadatas ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

user = getUser(conn, myEnv.rodsUserName)

# Get a list of metadatas, a metadata is a tuple (name, value, units)
print user.getUserMetadata()
        
# Add some metadatas, the unit field is optional
user.addUserMetadata("test1", "value1")
user.addUserMetadata("test2", "value2", "units")
    
print user.getUserMetadata()
  
# Remove the metadatas we added
user.rmUserMetadata("test1", "value1")
user.rmUserMetadata("test2", "value2", "units")
    
print user.getUserMetadata()
    
conn.disconnect()
```
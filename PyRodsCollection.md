# How to work with Collections #

## Manage collections and files in collections ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

# These 2 resources have to exist
collName = "testCollection"        
resc1 = "demoResc"
resc2 = "demoResc2"

# Open the current working directory
c = irodsCollection(conn)

print c.getCollName()

c.createCollection("subCollection")
c.openCollection("subCollection")

print c.getCollName()

f = c.create("testCollection.txt")
nb_bytes_written = f.write("This is a test")
f.close()
# REPLICATE THE FILE AFTER CLOSING IT (BECAUSE MODE IS 'w')
f.replicate(resc2)    

f = c.create("testCollection2.txt", resc2)
nb_bytes_written = f.write("This is another test")
f.close()

print "Number of data objects :", c.getLenObjects()

print c.getObjects()

for dataObj in c.getObjects():
    data_name = dataObj[0]
    resc_name = dataObj[1]
    
    print data_name, resc_name
    
    f = c.open(data_name, "r", resc_name)
    
    print "  Path:", f.getCollName(), f.getDataName()
    print "  Resource Name :", f.getResourceName()
    print "  Repl Number :", f.getReplNumber()
    print "  Size :", f.getSize()
    print "  Content :", f.read()
    print 
    
    c.delete(data_name, resc_name)
    
    
c.upCollection()

print c.getCollName()
print "Number of subcollections :", c.getLenSubCollections()
print "  ", c.getSubCollections()


print "After deletion"
c.deleteCollection("subCollection")
print "Number of subcollections :", c.getLenSubCollections()
print "  ", c.getSubCollections()

c.upCollection()
c.deleteCollection(collName)

conn.disconnect()
```


## Manage metadatas ##


```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

path = myEnv.rodsHome

c = irodsCollection(conn, path)

# There's 2 way to deal with metadata

# 1: If you have an iRodsCollection object

# Add some metadata
c.addUserMetadata("units", "12", "cm")
c.addUserMetadata("author", "rods")
print c.getUserMetadata()    
c.rmUserMetadata("author", "rods")
c.rmUserMetadata("units", "12", "cm")    
print c.getUserMetadata()

# 2: If you have the irods path of the collection
addCollUserMetadata(conn, path, "units", "12", "cm")
addCollUserMetadata(conn, path, "author", "rods")
print getCollUserMetadata(conn, path)
rmCollUserMetadata(conn, path, "author", "rods")
rmCollUserMetadata(conn, path, "units", "12", "cm")
print getCollUserMetadata(conn, path)

conn.disconnect()
```
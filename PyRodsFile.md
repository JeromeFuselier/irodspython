# How to work with Files #

## Get file information ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

path = myEnv.rodsHome + '/testinfoio.txt'

# Open a file for writing
f = iRodsOpen(conn, path, 'w')
f.write("\/"*25)

print "Collection :", f.getCollName()
print "Data Name :", f.getDataName()
print "Desc Inx :", f.getDescInx()
print "Position :", f.getPosition()
print "Repl Number :", f.getReplNumber()
print "Resource Name :", f.getResourceName()
print "Size :", f.getSize()  # size = 0 because size is updated when you close the file

f.close()

f.delete()
 
conn.disconnect()
```


## Manage metadatas ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

path = myEnv.rodsHome + '/testmeta.txt'
 
f = iRodsOpen(conn, path, 'w')

# There's 2 way to deal with metadata

# 1: If you have an iRodsFile object

f.addUserMetadata("units", "12", "cm")
f.addUserMetadata("author", "rods")
print f.getUserMetadata()
f.rmUserMetadata("author", "rods")
f.rmUserMetadata("units", "12", "cm")
print f.getUserMetadata()        
    
# 2: If you have the irods path of the file
addFileUserMetadata(conn, path, "units", "12", "cm")
addFileUserMetadata(conn, path, "author", "rods")
print getFileUserMetadata(conn, path)
rmFileUserMetadata(conn, path, "author", "rods") 
rmFileUserMetadata(conn, path, "units", "12", "cm")
print getFileUserMetadata(conn, path)     
   
f.close()
f.delete()
 
conn.disconnect()
```


## Read/Write/Append ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

path = myEnv.rodsHome + '/testsimpleio.txt'

# Write a string in a file
f = iRodsOpen(conn, path, 'w')
f.write("This is a test")
f.close()

# Read the file
f = iRodsOpen(conn, path, 'r')
print f.read()
f.close()    

# Read the file (give the buffer size)
f = iRodsOpen(conn, path, 'r')
print f.read(5)
print f.read()
f.close()    

# Append to the file
f = iRodsOpen(conn, path, 'a')
f.write("\nThis is still the test")
f.close()

# Read the file again
f = iRodsOpen(conn, path, 'r')
print f.read()
f.close()
 
conn.disconnect()
```

## Seek ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

path = myEnv.rodsHome + '/testseekio.txt'

# Write a string in a file
f = iRodsOpen(conn, path, 'w')
f.write("-" * 100)
f.close()

# Read the file from several positions
f = iRodsOpen(conn, path, 'r')
print "Size :", f.getSize()
f.seek(50, os.SEEK_SET) # middle
print f.read()
    
f.seek(0) # begining    
print f.read()
f.seek(f.getSize(), os.SEEK_END) # begining (from the end)
print f.read()    
f.close()

# Modify the file 
f = iRodsOpen(conn, path, 'a')
f.seek(-60, os.SEEK_CUR)
print "Begin position of modify :", f.getPosition()
f.write("+" * 20)
print "End position of modify :", f.getPosition()       
f.close()   

# Read the modified file 
f = iRodsOpen(conn, path, 'r')
print f.read()    
f.close()

f.delete()
 
conn.disconnect()
```


## Access to replicate files ##

```
from irods import *

status, myEnv = getRodsEnv()
conn, errMsg = rcConnect(myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone)
status = clientLogin(conn)

# These 2 resources have to exist
resc1 = "demoResc"
resc2 = "demoResc2"
path = myEnv.rodsHome + '/testreplication.txt'

# If path exists on resc2, it will modify the version on resc2 and not create
# a new one on resc1. This is the choice of irods team.
f = iRodsOpen(conn, path, 'w', resc1)
f.write("=="*15)
f.close()

print "First read, both files are equal"

f = iRodsOpen(conn, path, 'r', resc1)
print "  Path:", f.getCollName(), f.getDataName()
print "  Resource Name :", f.getResourceName()
print "  Repl Number :", f.getReplNumber()
print "  Size :", f.getSize()
print "  Content :", f.read()
print
f.replicate(resc2)
f.close()

f = iRodsOpen(conn, path, 'r', resc2)
print "  Path:", f.getCollName(), f.getDataName()
print "  Resource Name :", f.getResourceName()
print "  Repl Number :", f.getReplNumber()
print "  Size :", f.getSize()
print "  Content :", f.read()
print
f.close()

print "Second read, first file is modified"

f = iRodsOpen(conn, path, 'a', resc1)
f.write("++"*15)
f.close()

f = iRodsOpen(conn, path, 'r', resc1)
print "  Path:", f.getCollName(), f.getDataName()
print "  Resource Name :", f.getResourceName()
print "  Repl Number :", f.getReplNumber()
print "  Size :", f.getSize()
print "  Content :", f.read()
print
f.close()

f = iRodsOpen(conn, path, 'r', resc2)
print "  Path:", f.getCollName(), f.getDataName()
print "  Resource Name :", f.getResourceName()
print "  Repl Number :", f.getReplNumber()
print "  Size :", f.getSize()
print "  Content :", f.read()
print
f.close()

print "Third read, synchronize the versions"

f.update()

f = iRodsOpen(conn, path, 'r', resc1)
print "  Path:", f.getCollName(), f.getDataName()
print "  Resource Name :", f.getResourceName()
print "  Repl Number :", f.getReplNumber()
print "  Size :", f.getSize()
print "  Content :", f.read()
print
f.close()

f = iRodsOpen(conn, path, 'r', resc2)
print "  Path:", f.getCollName(), f.getDataName()
print "  Resource Name :", f.getResourceName()
print "  Repl Number :", f.getReplNumber()
print "  Size :", f.getSize()
print "  Content :", f.read()
print
f.close()

print "Get the replicas"
f = iRodsOpen(conn, path, 'r', resc1)

for fi in f.getReplications():
    print "  Path:", fi.getCollName(), fi.getDataName()
    print "  Resource Name :", fi.getResourceName()
    print "  Repl Number :", fi.getReplNumber()
    print "  Size :", fi.getSize()
    print "  Content :", fi.read()
    print
    
    fi.close()
    fi.delete()    
    
f.close()
 
conn.disconnect()
```
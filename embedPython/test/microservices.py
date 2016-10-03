import sys, os
import smtplib
from email.mime.text import MIMEText
from irods import *
import commands


################################################
# Read a file from iRODS and returns the string
################################################
def open(conn, irods_path):
    f = iRodsOpen(conn, irods_path, 'r')            
    str_xml = f.read()    
    f.close()
    return str_xml


####################################
# Write a string to a file on iRODS
####################################
def write(conn, str, irods_path):
    f = iRodsOpen(conn, irods_path, 'w')
    f.write(str)
    f.close()
    
 
##############
# Send a mail
##############   
def send_mail(str, email):
    msg = MIMEText(str)
    s = smtplib.SMTP()
    s.connect()
    s.sendmail(email, [email], msg.as_string())
    s.close()

    
  
####################################  
## A microservice with no parameter  
####################################
def test_python(rei):
    # Send a local mail
    send_mail("Test of the python microservice without parameters", 
              'rods@localhost')
              

    
###################################
## A microservice with 1 parameter 
###################################
def test_python1(param1, rei):
    # Open the file passed in parameter and send the content by email
        
    in_path = param1.parseForStr()
    f = iRodsOpen(rei.getRsComm(), in_path)    
    if not f:
        str_doc = "File : %s not found" % in_path
    else:
        str_doc = f.read()
    send_mail(str_doc, 'rods@localhost')
    
    
####################################
## A microservice with 2 parameters 
####################################   
def test_python2(param1, param2, rei):
    num = param1.parseForPosInt()
    fillIntInMsParam(param2, num*2)
    
    
####################################
## A microservice with 3 parameters 
####################################   
def test_python3(param1, param2, param3, rei):
    s1 = param1.parseForStr()
    s2 = param2.parseForStr()
    fillStrInMsParam(param3, s1 + ' - ' + s2)
    
    
####################################
## A microservice with 4 parameters 
####################################   
def test_python4(param1, param2, param3, param4, rei):
    i1 = param1.parseForPosInt()
    s1 = param2.parseForStr()
    fillIntInMsParam(param3, i1 * 2)
    fillStrInMsParam(param4, "** " + s1 + " **")
    
    
####################################
## A microservice with 5 parameters 
####################################   
def test_python5(param1, param2, param3, param4, param5, rei):
    i1 = param1.parseForPosInt()
    i2 = param2.parseForPosInt()
    i3 = param3.parseForPosInt()
    i4 = param4.parseForPosInt()
    fillIntInMsParam(param5, i1 + i2 + i3 * i4)
    
    
####################################
## A microservice with 6 parameters 
####################################   
def test_python6(param1, param2, param3, param4, param5, param6, rei):
    s1 = param1.parseForStr()
    s2 = param2.parseForStr()
    s3 = param3.parseForStr()
    s4 = param4.parseForStr()
    s5 = param5.parseForStr()
    
    d = eval(s3)
    l1 = eval(s4)
    l2 = eval(s5)
    d[s1] = l1
    d[s2] = l2
    
    fillStrInMsParam(param6, str(d))
    
    
####################################
## A microservice with 7 parameters 
####################################   
def test_python7(param1, param2, param3, param4, param5, param6, param7, rei):
    l = [param1.parseForStr(),
         param2.parseForStr(),
         param3.parseForStr(),
         param4.parseForStr(),
         param5.parseForStr(),
         param6.parseForStr()]
        
    fillStrInMsParam(param7, str(" - ".join(l)))
    
    
####################################
## A microservice with 8 parameters 
####################################   
def test_python8(param1, param2, param3, param4, param5, param6, param7, param8, rei):
    l = [param1.parseForStr(),
         param2.parseForStr(),
         param3.parseForStr(),
         param4.parseForStr(),
         param5.parseForStr(),
         param6.parseForStr(),
         param7.parseForStr()]
        
    fillStrInMsParam(param8, str(" - ".join(l)))
    
    
#################
## Collections ##
#################


def msiTestCollectionMetadata(path, resStr, rei):    
    c = irodsCollection(rei.getRsComm(), path.parseForStr())
 
    res = ''
    
    if c:
        c.addUserMetadata("units", "12", "cm")
        c.addUserMetadata("author", "jerome")
        res += str(c.getUserMetadata())
    
        c.rmUserMetadata("author", "jerome")
        res += str(c.getUserMetadata())     
    
        c.rmUserMetadata("units", "12", "cm")    
        res += str(c.getUserMetadata())
           
    fillStrInMsParam(resStr, str(res))
   
   
    
def msiTestCollection(rescName1, rescName2, resStr, rei):  
        
    resc1 = rescName1.parseForStr()
    resc2 = rescName2.parseForStr()
    
    res = ''
    
    # Open the current working directory
    c = irodsCollection(rei.getRsComm())
    
    res += c.getCollName() + '\n'

    c.createCollection("testCollection")
    c.openCollection("testCollection")
    
    res += c.getCollName() + '\n'
    
    f = c.create("testCollection.txt")
    nb_bytes_written = f.write("This is a test")
    f.close()
    # REPLICATE THE FILE AFTER CLOSING IT (BECAUSE MODE IS 'w')
    f.replicate(resc2)    

    f = c.create("testCollection2.txt", resc2)
    nb_bytes_written = f.write("This is another test")
    f.close()
    
    res += "Number of data objects :%d\n %s\n" % (c.getLenObjects(), str(c.getObjects()))
    
    
    for dataObj in c.getObjects():
        data_name = dataObj[0]
        resc_name = dataObj[1]
        
        res += data_name + ' ' + resc_name + '\n'
        
        f = c.open(data_name, "r", resc_name)
        
        res += "  Path:" +  f.getCollName() + '/' + f.getDataName() + '\n'
        res += "  Resource Name :" + f.getResourceName() + '\n'
        res += "  Repl Number :" + str(f.getReplNumber()) + '\n'
        res += "  Size :" + str(f.getSize()) + '\n'
        res += "  Content :" + f.read() + '\n\n'        
        
        c.delete(data_name, resc_name)
        
        
    c.upCollection()
    
    res += c.getCollName() + '\n'
    res += "Number of subcollections :%d\n %s\n" % (c.getLenSubCollections(), str(c.getSubCollections()))
    
    
    res += "After deletion\n"
    c.deleteCollection("subCollection")
    res += "Number of subcollections :%d\n %s\n" % (c.getLenSubCollections(), str(c.getSubCollections()))
           
           
    fillStrInMsParam(resStr, str(res))
    
###########
## Files ##
###########

def msiTestFileMetadata(path, resStr, rei):
    res = '' 
    
    f = iRodsOpen(rei.getRsComm(), path.parseForStr(), 'w')
     
    if f:             
        # Add some metadata
        f.addUserMetadata("units", "12", "cm")
        f.addUserMetadata("author", "jerome")
        res += str(f.getUserMetadata())
        f.rmUserMetadata("author", "jerome")
        res += str(f.getUserMetadata()) 
       
        f.close()
        f.delete()
    
    else:
        res += "File", path.parseForStr(), "does not exist"
        
    fillStrInMsParam(resStr, str(res))
     
    

def msiTestReadWriteAppend(inPath, resStr, rei):  
    
    path = inPath.parseForStr()
    res = ''
    
    # Write a string in a file
    f = iRodsOpen(rei.getRsComm(), path, 'w')
    f.write("This is a test") 
    f.close()
    
    # Read the file
    f = iRodsOpen(rei.getRsComm(), path, 'r')
    res +=  f.read() + '\n'
    f.close()    
    
    # Read the file (give the buffer size)
    f = iRodsOpen(rei.getRsComm(), path, 'r')
    res += f.read(5) + '\n'
    res += f.read() + '\n'
    f.close()    
    
    # Append to the file
    f = iRodsOpen(rei.getRsComm(), path, 'a')
    f.write("\nThis is still the test")
    f.close()

    # Read the file again
    f = iRodsOpen(rei.getRsComm(), path, 'r')
    res += f.read()
    f.close()    
    
    fillStrInMsParam(resStr, str(res))
     
    
    
def msiTestSeek(inPath, resStr, rei):  
    
    path = inPath.parseForStr()
    res = ''    
    
    # Write a string in a file
    f = iRodsOpen(rei.getRsComm(), path, 'w')
    f.write("-" * 100)
    f.close()

    # Read the file from several positions
    f = iRodsOpen(rei.getRsComm(), path, 'r')
    res +=  "Size :%d\n" % f.getSize()
    f.seek(50, os.SEEK_SET) # middle
    res += f.read() + '\n'
        
    f.seek(0) # begining    
    res += f.read() + '\n'
    f.seek(f.getSize(), os.SEEK_END) # begining (from the end)
    res += f.read()    + '\n' 
    f.close()
    
    # Modify the file 
    f = iRodsOpen(rei.getRsComm(), path, 'a')
    f.seek(-60, os.SEEK_CUR) # middle
    res += "Begin position of modify : %d\n" % f.getPosition()
    f.write("+" * 20)
    res += "End position of modify : %d\n" % f.getPosition()   
    f.close()   
    
    # Read the modified file 
    f = iRodsOpen(rei.getRsComm(), path, 'r')
    res += f.read()   + '\n'  
    f.close()
    
    f.delete()
    
    fillStrInMsParam(resStr, str(res))
    
    
    
def msiGetFileInfos(path, resStr, rei):
    
    f = iRodsOpen(rei.getRsComm(), path.parseForStr(), 'w')
    f.write("\/"*25)
    
    res = (f.getCollName(),
           f.getDataName(),
           f.getDescInx(),
           f.getPosition(),
           f.getReplNumber(),
           f.getResourceName(),
           f.getSize() )  # size = 0 because size is updated when you close the file
    
    f.close()
    
    f.delete()
    
    
    fillStrInMsParam(resStr, str(res))
    
    
def msiTestReplication(inPath, resc1, resc2, resStr, rei):
    
    path = inPath.parseForStr()
    
    res = ''
    
    
    # If path exists on resc2, it will modify the version on resc2 and not create
    # a new one on resc1. This is the choice of irods team.
    f = iRodsOpen(rei.getRsComm(), path, 'w', resc1.parseForStr())
    f.write("=="*15)
    f.close()
    
    res += "First read, both files are equal\n"
    
    f = iRodsOpen(rei.getRsComm(), path, 'r', resc1.parseForStr())
    res += "  Path: %s/%s\n" % (f.getCollName(), f.getDataName())
    res += "  Resource Name :" + f.getResourceName() + '\n'
    res += "  Repl Number : %d\n" % f.getReplNumber()
    res += "  Size :" + str(f.getSize()) + '\n'
    res += "  Content :" +  f.read() + '\n\n'
    f.replicate(resc2.parseForStr())
    f.close()
    
    f = iRodsOpen(rei.getRsComm(), path, 'r', resc2.parseForStr())
    res += "  Path: %s/%s\n" % (f.getCollName(), f.getDataName())
    res += "  Resource Name :" + f.getResourceName() + '\n'
    res += "  Repl Number : %d\n" % f.getReplNumber()
    res += "  Size :" + str(f.getSize()) + '\n'
    res += "  Content :" + f.read() + '\n\n'
    f.close()
    
    
    res += "Second read, first file is modified\n"
    
    f = iRodsOpen(rei.getRsComm(), path, 'a', resc1.parseForStr())
    f.write("++"*15)
    f.close()
    
    f = iRodsOpen(rei.getRsComm(), path, 'r', resc1.parseForStr())
    res += "  Path: %s/%s\n" % (f.getCollName(), f.getDataName())
    res += "  Resource Name :" + f.getResourceName() + '\n'
    res += "  Repl Number : %d\n" % f.getReplNumber()
    res += "  Size :" + str(f.getSize()) + '\n' 
    res += "  Content :" + f.read() + '\n\n'
    f.close()
    
    f = iRodsOpen(rei.getRsComm(), path, 'r', resc2.parseForStr())
    res += "  Path: %s/%s\n" % (f.getCollName(), f.getDataName())
    res += "  Resource Name :" + f.getResourceName() + '\n'
    res += "  Repl Number : %d\n" % f.getReplNumber()
    res += "  Size :" + str(f.getSize()) + '\n'
    res += "  Content :" + f.read() + '\n\n'
    f.close()
    
    res += "Third read, synchronize the versions\n"
    
    f.update()
    
    f = iRodsOpen(rei.getRsComm(), path, 'r', resc1.parseForStr())
    res += "  Path: %s/%s\n" % (f.getCollName(), f.getDataName())
    res += "  Resource Name :" + f.getResourceName() + '\n'
    res += "  Repl Number : %d\n" % f.getReplNumber()
    res += "  Size :" + str(f.getSize()) + '\n'
    res += "  Content :" + f.read() + '\n\n'
    f.close()
    
    f = iRodsOpen(rei.getRsComm(), path, 'r', resc2.parseForStr())
    res += "  Path: %s/%s\n" % (f.getCollName(), f.getDataName())
    res += "  Resource Name :" + f.getResourceName() + '\n'
    res += "  Repl Number : %d\n" % f.getReplNumber()
    res += "  Size :" + str(f.getSize()) + '\n'
    res += "  Content :" + f.read() + '\n\n'
    f.close()
    
    res += "Get the replicas\n"
    
    f = iRodsOpen(rei.getRsComm(), path, 'r', resc1.parseForStr())
    
    for fi in f.getReplications():
        res += "  Path: %s/%s\n" % (fi.getCollName(), fi.getDataName())
        res += "  Resource Name :" + fi.getResourceName() + '\n'
        res += "  Repl Number : %d\n" % fi.getReplNumber()
        res += "  Size :" + str(fi.getSize()) + '\n'
        res += "  Content :" + fi.read() + '\n\n'
        
        fi.close()
        fi.delete()    
        
    f.close()
    
    
    fillStrInMsParam(resStr, str(res))
    
    
############    
## Groups ##
############


def msiSetGroupMetadata(groupName, name, value, units, rei):
    group = getGroup(rei.getRsComm(), groupName.parseForStr())
    
    if group:        
        return group.addUserMetadata(name.parseForStr(), 
                                     value.parseForStr(),
                                     units.parseForStr())
    else:
        return -1
    
    
def msiGetGroupMetadata(groupName, metadatas, rei):
    group = getGroup(rei.getRsComm(), groupName.parseForStr())
    
    res = []
        
    if group:        
        res = group.getUserMetadata()
    
    fillStrInMsParam(metadatas, str(res))
    
    
def msiRmGroupMetadata(groupName, name, value, units, rei):
    group = getGroup(rei.getRsComm(), groupName.parseForStr())
        
    if group:        
        return group.rmUserMetadata(name.parseForStr(), 
                                   value.parseForStr(),
                                   units.parseForStr())
    else:
        return -1 
    
    
    
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
    
    
def msiGetGroups(groups, rei):
    fillStrInMsParam(groups, str([g.getName() for g in getGroups(rei.getRsComm())]))
    
    
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

    
###############
## Resources ##
###############

     
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
    

def msiGetResources(resources, rei):
    fillStrInMsParam(resources, str([r.getName() for r in getResources(rei.getRsComm())]))
    
    
    
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


   
def msiSetResourceMetadata(rescName, name, value, units, rei):
    resc = getResource(rei.getRsComm(), rescName.parseForStr())
    
    if resc:        
        return resc.addUserMetadata(name.parseForStr(), 
                                    value.parseForStr(),
                                    units.parseForStr())
    else:
        return -1
    
    
def msiGetResourceMetadata(rescName, metadatas, rei):
    resc = getResource(rei.getRsComm(), rescName.parseForStr())
    
    res = []
        
    if resc:        
        res = resc.getUserMetadata()
    
    fillStrInMsParam(metadatas, str(res))
    
    
def msiRmResourceMetadata(rescName, name, value, units, rei):
    resc = getResource(rei.getRsComm(), rescName.parseForStr())
        
    if resc:        
        return resc.rmUserMetadata(name.parseForStr(), 
                                   value.parseForStr(),
                                   units.parseForStr())
    else:
        return -1
    
########### 
## Users ##
###########


def msiSetUserMetadata(userName, name, value, units, rei):
    user = getUser(rei.getRsComm(), userName.parseForStr())
    
    if user:        
        return user.addUserMetadata(name.parseForStr(), 
                                    value.parseForStr(),
                                    units.parseForStr())
    else:
        return -1
    
    
def msiGetUserMetadata(userName, metadatas, rei):
    user = getUser(rei.getRsComm(), userName.parseForStr())
    
    res = []
        
    if user:        
        res = user.getUserMetadata()
    
    fillStrInMsParam(metadatas, str(res))
    
    
def msiRmUserMetadata(userName, name, value, units, rei):
    user = getUser(rei.getRsComm(), userName.parseForStr())
        
    if user:        
        return user.rmUserMetadata(name.parseForStr(), 
                                   value.parseForStr(),
                                   units.parseForStr())
    else:
        return -1 
    
    
    
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
    
    
def msiGetUsers(users, rei):
    fillStrInMsParam(users, str([g.getName() for g in getUsers(rei.getRsComm())]))
    
    
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


###########
## Zones ##
###########

   
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
    

def msiGetZones(zones, rei):
    fillStrInMsParam(zones, str([z.getName() for z in getZones(rei.getRsComm())]))
    
  
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


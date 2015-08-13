It is interesting to note that the code you write for a client python application can be easily transformed in a microservice if you replace the connection obtained by the rcConnect method with the connection obtained via the rei structure. All python objects are smart enough to adapt their behaviour according to the connection parameter used.

# The Microservice file #

The file is saved in /home/rods/microservices/process.py, on the server where iRODS is running.
Alternatively, you can iput this file in iRODS, for instance in /tempZone/home/rods/microservices/process.py.

```
import sys, os
import smtplib
from email.mime.text import MIMEText
from irods import *


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

    
def test_python(rei):
    # Send a local mail
    send_mail("Test of the python microservice without parameters", 
              'rods@localhost')
              
    
def test_python1(param1, rei):
    # Open the file passed in parameter and send the content by email
        
    in_path = param1.parseForStr()
    f = iRodsOpen(rei.getRsComm(), in_path)    
    if not f:
        str_doc = "File : %s not found" % in_path
    else:
        str_doc = f.read()
    send_mail(str_doc, 'rods@localhost')
    
    
def test_python2(param1, param2, rei):
    num = param1.parseForPosInt()
    fillIntInMsParam(param2, num*2)
    
    
def test_python3(param1, param2, param3, rei):
    s1 = param1.parseForStr()
    s2 = param2.parseForStr()
    fillStrInMsParam(param3, s1 + ' - ' + s2)
    
    
def test_python4(param1, param2, param3, param4, rei):
    i1 = param1.parseForPosInt()
    s1 = param2.parseForStr()
    fillIntInMsParam(param3, i1 * 2)
    fillStrInMsParam(param4, "** " + s1 + " **")
    
    
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
```



# A simple test #

The _test.ir_ file contains

```
test||msiPyInitialize##msiLocalPython(*pyScript, *methName, noTestRecursion)##msiPyFinalize|nop
*pyScript=/home/rods/microservices/process.py%*methName=test_python
null
```

You execute the code with **irule -vF test.ir**

The code should send an email to the local rods user.

Alternatively, you can use this version for the script stored in iRODS :

```
test||msiPyInitialize##msiRodsPython(*pyScript, *methName, noTestRecursion)##msiPyFinalize|nop
*pyScript=/tempZone/home/rods/microservices/process.py%*methName=test_python
null
```

All the following examples work for both versions.


# Using parameters #

The _test.ir_ file contains

```
test||msiPyInitialize##msiLocalPython1(*pyScript, *methName, noTestRecursion, *irodsPath)##msiPyFinalize|nop
*pyScript=/home/rods/microservices/process.py%*methName=test_python1%*irodsPath=/tempZone/home/cheshire/test.txt
null
```

This script uses an irods path as parameter and sends the content of the file to the rods local user.

# Working on integer parameters #

The _test.ir_ file contains

```
test||msiPyInitialize##msiLocalPython2(*pyScript, *methName, noTestRecursion, 10, *res)##msiPyFinalize|nop
*pyScript=/home/rods/microservices/process.py%*methName=test_python2
*res
```

The script parses an integer parameter and returns another integer parameter.


# Working on string parameters #

The _test.ir_ file contains

```
test||msiPyInitialize##msiLocalPython3(*pyScript, *methName, noTestRecursion, "first part", "second part", *res)##msiPyFinalize|nop
*pyScript=/home/rods/microservices/process.py%*methName=test_python3
*res
```

The script parses two string parameters and returns a string which concatenates the two.


# Working with several parameters #

The _test.ir_ file contains

```
test||msiPyInitialize##msiLocalPython4(*pyScript, *methName, noTestRecursion, 10, "str", *res1, *res2)##msiPyFinalize|nop
*pyScript=pyScript=/home/rods/microservices/process.py%*methName=test_python4
*res1%*res2
```

An example with two input parameters and two output parameters.



# Using simple python objects parameters #

The _test.ir_ file contains

```
test||msiPyInitialize##msiLocalPython6(*pyScript, *methName, noTestRecursion, a, b, "{'c':'12'}", "[1,2,3]", "[3,4,5]", *res)##msiPyFinalize|nop
*pyScript=/home/rods/microservices/process.py%*methName=test_python6
*res
```

An example with python structures as parameters. You can use the **eval** and **str** method to pass python objects as string parameters in microservices.


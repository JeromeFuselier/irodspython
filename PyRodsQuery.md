# How to work with Queries #

## Get the users of a specific zone ##

```
from irods import *
from irods_error import *


def printGenQueryResults(conn, status, genQueryOut):        
    if status != 0:
        if status == CAT_NO_ROWS_FOUND:
            print "No result"
        else:
            print "Error : rcGenQuery - %d" % status
    else :
        sqlResults = genQueryOut.getSqlResult()
           
        res = [ sqlResult.getValues() for sqlResult in sqlResults ]
            
        for r in xrange(genQueryOut.getRowCnt()):
            for a in xrange(genQueryOut.getAttriCnt()):
                print res[a][r],
            print


# Connection
myEnv, status = getRodsEnv()
conn, errMsg = rcConnect(myEnv.getRodsHost(), myEnv.getRodsPort(), myEnv.getRodsUserName(), myEnv.getRodsZone())
status = clientLogin(conn)
       
genQueryInp = genQueryInp_t()

# Add the field we want to extract, this will be the attributes column in the
# genQueryOut variable
i1 = inxIvalPair_t()    
i1.addInxIval(COL_USER_NAME, 0)  # user name
i1.addInxIval(COL_USER_ZONE, 0)  # user zone

# Add some conditions to the query    
i2 = inxValPair_t()
i2.addInxVal(COL_USER_ZONE, "='tempZone'") # user of the tempZone only
i2.addInxVal(COL_USER_TYPE, "<>'rodsgroup'") # not a group (groups are in the user table)
    
genQueryInp.setSelectInp(i1)
genQueryInp.setSqlCondInp(i2)

genQueryInp.setMaxRows(10)
genQueryInp.setContinueInx(0)

# Query the icat    
genQueryOut, status = rcGenQuery(conn, genQueryInp)
       
printGenQueryResults(conn, status, genQueryOut)

# If there are more than maxRox result we need to iterate    
while status == 0 and genQueryOut.getContinueInx() > 0:
    genQueryInp.setContinueInx(genQueryOut.getContinueInx())
    genQueryOut, status = rcGenQuery(conn, genQueryInp)          
    printGenQueryResults(conn, status, genQueryOut)

conn.disconnect()
```


## Get files which match the conditions ##

```
from irods import *
from irods_error import *


def printGenQueryResults(conn, status, genQueryOut):        
    if status != 0:
        if status == CAT_NO_ROWS_FOUND:
            print "No result"
        else:
            print "Error : rcGenQuery - %d" % status
    else :
        sqlResults = genQueryOut.getSqlResult()
           
        res = [ sqlResult.getValues() for sqlResult in sqlResults ]
            
        for r in xrange(genQueryOut.getRowCnt()):
            for a in xrange(genQueryOut.getAttriCnt()):
                print res[a][r],
            print


# Connection
myEnv, status = getRodsEnv()
conn, errMsg = rcConnect(myEnv.getRodsHost(), myEnv.getRodsPort(), myEnv.getRodsUserName(), myEnv.getRodsZone())
status = clientLogin(conn)


genQueryInp = genQueryInp_t()

# Select the attribute we want, order the result by data size 
i1 = inxIvalPair_t()    
i1.addInxIval(COL_COLL_NAME, 0)
i1.addInxIval(COL_DATA_NAME, 0)
i1.addInxIval(COL_DATA_SIZE, ORDER_BY)

# Add some conditions
i2 = inxValPair_t()
i2.addInxVal(COL_D_OWNER_NAME, "='rods'")     # Owner of the data
i2.addInxVal(COL_DATA_SIZE, "> '2000'")
i2.addInxVal(COL_DATA_SIZE, "< '5000'")       # size between 2ko and 5ko
i2.addInxVal(COL_COLL_NAME, "like '%test%'")  # the string test should be in the path
 
genQueryInp.setSelectInp(i1)
genQueryInp.setSqlCondInp(i2)
genQueryInp.setMaxRows(10)
genQueryInp.setContinueInx(0)

# Execute the query 
genQueryOut, status = rcGenQuery(conn, genQueryInp)
printGenQueryResults(conn, status, genQueryOut)
 
while status == 0 and genQueryOut.getContinueInx() > 0:
    genQueryInp.setContinueInx(genQueryOut.getContinueInx())
    genQueryOut, status = rcGenQuery(conn, genQueryInp)          
    printGenQueryResults(conn, status, genQueryOut)

conn.disconnect()
```
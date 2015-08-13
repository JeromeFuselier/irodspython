# Requirements #

  * This module requires the Python-devel package, it has been tested with the 2.5/2.6 version of the interpreter.

  * The module needs the PyRods Api which wraps the C Api of the iRods server. It is possible to remove this dependency if you parse the msParam\_t parameters in the C microservice and pass the string or integer parameter when you call the Python method. It needs the Pyrods 2.1.0 version, with the msParam\_t binding.

# Installation #

The EmbedPythonInstall wiki page describes the recommended procedure to set up embedPython with an existing iRODS server and PyRods package.


# Usage #

  * In the test directory of the module there are a set of test files that can be used to check that the installation is working.

  * the `*`.ir files have to be called with the **irule -vF test`*`.ir** command, the results are either on the command line output or with local webmail (check the python code)

  * The rule workflows have to begin with the msiPyInitialize microservice and end with the msiPyFinalize microservice. The interpreter is common between each steps meaning that you can pass parameters between steps via the global dict.

  * All test files call methods which are present in the process.py file, the path of   the script is given as the first parameter of the microservice, the second parameter is the method name and the third one is the rule name (see RuleRecursion for some extra information on this parameter). The remaining parameters are the actual parameters of the python method.

  * The naming convention for the different available microservices (msiLocal, msiLocal1, msiRods1, ...) reflects the number of parameters in the python scripts.

  * The msiLocal`*` microservives takes as parameter a path of the script in the local filesystem of the server.

  * The msiRods`*`  microservives takes as parameter a path of the script in iRODS.

  * The last parameter of the python script (not counted in the naming convention) corresponds to the ruleExecInfo\_t parameter of microservices.

  * The parameters can be casted to strings or int with these calls :
    * param.parseForPosInt()
    * param.parseForStr()

  * The output parameters are returned with these calls :
    * fillIntInMsParam(param, 10)
    * fillStrInMsParam(param, "output")

  * The msiExecPython microservice execute a python string code. It can be used for instance to define variables or functions in the interpreter.

  * The EmbedPythonUsage shows the simple examples from the test directory

  * The EmbedPythonUser, EmbedPythonGroup, EmbedPythonResource, EmbedPythonZone, EmbedPythonFile and EmbedPythonCollection are the server-side examples of the Pyrods examples.


# ISSUES #

  * When you are using the delayExec microservice, the python interpreter is never closed (due to a bug in python 2.x). Thus if you modify a python microservice, the modification is not taken into account until you restart the server.

  * When you are using the delayExec microservice, there will be a bug in your python microservice if you try to execute commands.getoutput(...) or os.popen(...) functions to execute another process. You should use the following code :

```
def msiTestCmd(ruleName, rei):
    cmd = 'echo "OK : %s"' % ruleName.parseForStr()
    pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout    
    result = pipe.read()
    pipe.close()
    
    print result
```

  * The package is an iRODS module. In order to simplify the installation, we decided to not modify iRODS source code. Thus we had to stick with the existing error codes.

  * If you get USER\_FILE\_DOES\_NOT\_EXIST error, this should probably means that the module was not able to load the script which contains microservices. It's probably that the path to your python script does not exist or that there is a syntax error (like indent problem). The error should be available in the rodsLog file.

  * If you get NO\_MICROSERVICE\_FOUND\_ERR error, this means that the module was not able to find the python microservice in the script you selected. Check the function name of the call (the second parameter in msiLocalPython...)

  * If you get INVALID\_OBJECT\_TYPE error, this means that the callFunction of the C Api failed. This happens for instance if the syntax of the python microservice is wrong or if you use undefined variables. The traceback should be printed in the rodsLog logfile with more precise information.



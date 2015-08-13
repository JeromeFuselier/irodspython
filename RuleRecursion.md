# Rule recursion #

  * For the moment, irods rule engine is not able to detect a recursion from the set of rules defined in the _core.irb_ file. This may happen quite quickly if you work a lot with hooks.

  * EmbedPython provides a simple way to avoid some recursive situation with the third parameter of the msiLocal`*` microservices. Each time you call a microservice, the rulename, the name of the python microservice, the parameters of the methods and the rei structure is keeped in memory during all the life of the irodsAgent. If the same call is already found in the list of previous calls the microservice is not executed.

  * This solution does not work perfectly well yet. When you consider periodic execution of the rules, this fails because the process is always running and the rei structures are disposed regularly.

  * If you want to remove the recursion check, you can use **noRecursionTest** as the rule name and this will skip the test.
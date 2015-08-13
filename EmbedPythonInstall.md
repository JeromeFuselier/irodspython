# Installation #


  * Expand the package in the modules directory of the iRODS installation

  * Adapt the Makefile to match your current directories and paths for the different libraries and object files.

  * Enable the microservice with the 'enabled' parameter in the info.txt file. You can also have to do a `./scripts/configure --enable-embedPython` in the iRODS root directory.

  * Run `make` in the iRODS root directory and hopefully you will only have warnings and no errors.

  * You will have to rebuild PyRods as object files are statically linked. (You will have to remove irods.so if you want to force the linking step)
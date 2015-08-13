# INSTALLATION #

  * Expand the package

  * Apply the irods Patch (See Patching iRODS below).

  * Modify the **irods\_dir** in the _setup.cfg_ file or use the **"--irods-dir="** option when building the library. This should point to your local install of irods. It will be used to find object and config files from your irods server.

  * The script assumes that you are using a _postgres_ database (the default choice when you install irods). If you are using another system you will have to adapt the _setup.py_ script to match your configuration.

  * Run **python setup.py build**

  * The _irods.so_ and _irods\_error.so_ libraries will be built in the _build/lib-`*`_directory.

  * Optionally, you can install the libraries in your python site-packages with the **python setup.py install** command. You will need the permission to write in that directory.



# PATCHING IRODS #

  * The 2.1 irods version needs to be patched in order to correct minor bugs. These bugs will be corrected with the next version.

  * The patch file _patch`_`08`_`14`_`09.patch_ keeps the differences between the vanilla source tree of the server and the modified one. It has been created with the diff command (**diff -rupN original/ new/ > patch\_08\_14\_09.patch**).

  * You can apply the patch manually by modifying the 4 files or use the gnu patch  command. To use the patch command you need to copy the patch file in your irods directory and execute the command **patch -p1 < patch\_08\_14\_09.patch**

  * There are 4 files that need to be modified :
    * _lib/core/include/rodsGenQuery.h_
    * _lib/core/include/rodsGenQueryNames.h_
    * _server/icat/src/icatGeneralQuery.c_
    * _server/icat/src/icatGeneralQuerySetup.c_

  * Execute **make** to recompile iRODS.

# KNOWN ISSUES #


  * The package needs to link with server object files, this means that you may need to add a link to object files for your installed microservices modules in the _setup.py_ script. You will have an error when you try to do import irods in a python shell after installing the package, it will complain for a missing object file for the module. If all goes well the script parses the config.mk file in the irods directory to find which module is linked with the server. It assumes a standard hierarchy (_modules/modulename/obj/`*`.o_), if this is not the case you will have to modify the install script.

  * The package links statically with server object files, this means that you have to do the link again if the irods object files are modified (and by propagation if you modify a C module which contains microservices). The fastest way to do that is to remove the _irods.so_ file in the _build/lib.../_ directory and execute **python setup.py build**, it will the just do the linking part.
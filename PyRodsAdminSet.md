# Presentation #

PyRodsAdminSet is a set of two administration interfaces, an administration tool and a more specific rule designer. The rule designer can be launched from the administration interface or as a standalone application, it only works for the old rule syntax and is not up-to-date.

## Administration interface: PyRodsAdmin ##

The administration interface is an iRODS browser written in Python and working under linux. It provides a high-level interface to manage an iRODS server :

  * Navigation through collections.

  * Upload/Download/Replicate files or collections.

  * Get user information.

  * Manage files/collections metadatas.

Below is a snapshot of the latest version of the interface :

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/PyRodsAdmin.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/PyRodsAdmin.png)

## iRule Designer: PyRuleDesigner (**Deprecated**) ##

The iRule Designer is a tool which allows the graphical design of iRODS irules. It can be linked to an existing iRODS server to directly access the `*`.irb files that are in use. It is also capable to import and export irb files from another location.

Below is a snapshot of the latest version of the interface in the package PyrodsAdmin-1.1.5.tar.gz:

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/iRuleDesigner.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/iRuleDesigner.png)


# Requirements #

  * The Python code in itself is working with previous python version but it relies on a module which requires [Python 2.6](http://www.python.org).

  * The interface has been created with [wxPython 2.8](http://wxpython.org). It has only been tested with the latest version 2.8.10.1 but it should work with older versions.

  * [LEPL](http://code.google.com/p/lepl) is a parser generator. It is used to parse the rules and create an abstract tree.

  * The rule designer (design.py) can be used without iRODS but the admin interface (main.py) requires the PyRods binding interface which corresponds to your iRODS version.

  * A configured [iRODS](https://www.irods.org) server is needed by the admin interface to access collections and data objects.

  * The code has been tested under Linux. The designer works under Windows, to use PyRodsAdmin you will need to compile PyRods for windows (it may be possible but it's not in my priority list).


# Installation #

  * Decompress the **PyRodsAdmin-version.tar.gz** archive somewhere.

  * Modify the **config.ini** file to match your current configuration. (See PyRodsAdminConfiguration)

  * Launch the admin interface with **python main.py**. (See PyRodsAdmin)

  * ~~Launch the rule designer with **python design.py**.~~ (See PyRuleDesigner) (Not present anymore after 1.1.5 version)

# News #

  * version 1.1.6:
    * Update for iRODS 3.2 and PyRods 3.2.3

  * version 1.1.4 :
    * Drag files from nautilus (or other file manager) to iRODS
    * Drag files from iRODS to a local directory (menu View/Local Filesystem)
    * Transfer DialogBox to inform the user for large transfer

  * version 1.1.3 :
    * Edit recovery part of the rule

  * version 1.1.2 :
    * Delete entire rule
    * Wizard to modify microservices.ini file for a specific module

  * version 1.1.0:
    * Add wizards for rules and microservices
    * Add two buttons for modifying the order of rules
    * Copy/Paste a list of microservices
    * Correct some bugs in the parser
    * Improve the look and feel
    * Add live validation with icons
# Presentation #

  * PyRodsAdmin is an administration tool for an iRODS server. It consists in an iRODS Browser and an iRule Designer (the designer is described in PyRuleDesigner). The goal is to provide a high-level interface to manage an iRODS server.

  * The application requires a connection to a running iRODS server to function. You can launch it with the command **python main.py**


> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/PyRodsAdmin.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/PyRodsAdmin.png)

### Connection screen ###

  * The connection screen allows the user to enter information for a specific server. It can be bypass with the _quick`_`connect_ option in the configuration file (See PyRodsAdminConfiguration).

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/login.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/login.png)


### Navigation through data objects ###

  * The collection tree shows the collection hierarchy of the server.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/collection_tree.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/collection_tree.png)


  * The file list displays the information we have on data objects from the iCAT.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/file_list.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/file_list.png)


### Manage Data objects ###

  * The input/output toolbar is used to upload or download files and collections from a server. The resource combo list can be used to specified what resource we are considering.

  * Download and upload files.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/tb_file.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/tb_file.png)

  * Download and upload collections.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/tb_collection.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/tb_collection.png)

  * Select the default resource.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/tb_resource.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/tb_resource.png)


### Get User information ###

  * The user toolbar can be used to display information on the connected user.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/user.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/user.png)


### Manage metadatas ###

  * The interface propose two similar components to add/delete metadatas for data objects and collections.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/metadata.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/metadata.png)


### Drag & Drop support ###

  * You can drag a list of files or directories from your file manager directly to the interface. It works if you drop it in the file list of the current collection or directly in the tree view of iRODS collection.
  * To drop a file or a collection from iRODS to a local directory you have to display the local directories tree with the menu View/Local File System and drop your selection in this component.
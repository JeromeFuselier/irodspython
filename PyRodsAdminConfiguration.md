# Introduction #

The application is configured with the **config.ini** file in the root directory. It uses the Python [ConfigParser](http://docs.python.org/library/configparser.html) class to parse the file. The structure is similar to what you would find on Microsoft Windows INI files.

The file is shared by PyRodsAdmin and PyRuleDesigner. There's a section dedicated to iRODS configuration and other sections dedicated to the appearance of objects in the canvas.

## iRods Configuration ##

```
[iRods Options]

quick_connect = true
username = rods
hostname = localhost
zone = tempZone
port = 1247
path = /home/rods/install/iRODS
```

  * The _quick`_`connect_ option is used by PyRodsAdmin. If set to true the connection will be simplified and will use the information provided with the options _username_, _hostname_, _zone_ and _port_. The password will then be retrieved from the _.irodsA_ file.

  * The _path_ points to the local installation of iRODS. It is used to locate the `*`.irb files which are loaded by the iRule Designer. If you want to use the tool without an iRODS server installed locally you just have to replace the line with _path=_ and the application will skip the import step.


## Appearance Configuration ##

### Colours ###

```
colour = (245, 222, 179)
```

  * Colours are represented by a triplet of RGB values. These are int between 0 and 255.


### Fonts ###

```
font = (12, False, False, "Liberation Serif")
```

  * Fonts are defined by a tuple parsed at launch time. Information are used to create font objects which are used to draw the shapes.
    1. **Size** of the font
    1. Use a **bold** font ?
    1. Use an **italic** font ?
    1. **face** name of the font.

  * The different fonts are defined in the "Fonts Options" section of the file.

  * In the utils directory there's a font\_selector.py script. It launches a small interface that can be used to browse the available fonts on the system. It shows a preview of the font and outputs the string that can be copied in the config.ini file. This is a snapshot of the application:

![http://irodspython.googlecode.com/svn/trunk/wiki/img/font_selector.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/font_selector.png)


### Canvas ###

  * The canvas section is used to define the background colour (_bg`_`colour_) of the canvas.


### Shapes ###

  * Shapes for microservices: The shape for a microservice is composed of an MsiShape and a MsiWorkflowShape. The MsiShape presents the name and the parameters of the microservice. The MsiWokflowShape is represented by a square next to a parameter of the microservice, it is used to show or close a subworkflow associated to the microservice (for instance the msiDelayExec defines two subworkflows).

![http://irodspython.googlecode.com/svn/trunk/wiki/img/msiShape.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/msiShape.png)

  * The shape for a rule: The RuleShape summarizes the four parts of an irule. It is updated when a modification is done on an element of the irule.

![http://irodspython.googlecode.com/svn/trunk/wiki/img/ruleShape.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/ruleShape.png)


## Wizard Configuration ##

  * Some wizards dialogs may be configured with config files to help the user for the creation of workflows. These files are in the doc directory.

### microservices.ini ###

  * A list of existing microservices, with description and parameters. The syntax is quite simple.

```
[msiCollCreate]
category = Collection Microservices
description = This microservice calls rsCollCreate to create a collection as part of a workflow execution.
p1_name = path
p1_desc = Data object path
p1_long_desc = A CollInp_MS_T or a STR_MS_T which would be taken as data object path
p1_types = STR_MS_T,CollInp_MS_T
p2_name = flags
p2_desc = Flags integer
p2_long_desc = A flags value of 1 means the parent collections will be created too.
p3_name = outStatus
p3_desc = Output status
p3_types = INT_MS_T
p3_values = 0,1
```

  * The category is used by the wizard to group the microservices in a tree widget.

  * the pn`_` options correspond to parameters where n is the number of the parameter. Only the name is mandatory. The remaining values are _desc_ and _long`_`desc_ for a small or more complete description. _types_ is the possible type for the parameter, it can be a comma-separated list. _values_ is a list of comma-separated possible values, the first one is the default.

  * The "tools/microservice\_manager.py" application can be used to edit this config file to add or remove available microservices. It can also load a _microservices.table_ to add microservices for an existing module.


### rules.ini ###

  * A list of existing rules, with description, parameters and initialized session variables. The syntax is similar than for the microservices file. You can't define values or types for parameters.

  * The sets of variables are defined in the session`_`variable.ini file.
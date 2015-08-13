# Introduction (**Deprecated**) #

  * PyRuleDesigner is a graphical tool used to design iRODS irules, funded under the PERICLES project. It can be configured to automatically load the `*`.irb files currently used by a local iRODS server, otherwise you can load an external file or create a new one. This is achieved with the _path_ parameter in the _config.ini_ file, the application parses the _path/server/config/server.config_ file to find the used `*`.irb files.

  * The application is launched with the command **python design.py** or by selecting the iRule perspective in the PyRodsAdmin interface.

  * **The rule designer supports up to .irb version 1.1.5**. Further versions are being developed as part of the PERICLES project.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/iRuleDesigner.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/iRuleDesigner.png)

### Loaded irb files ###

  * The application keeps a list of loaded irb files and display the found irules in the left list. The order of the list respects the order of the irules in the files, a different background color is used to display a visual separation for different irb files. When an irule is selected, the associated irb file path is displayed in the status bar. A right click on the list launches a contextual menu to add a new irule to a file.

  * The list is editable, you can select a specific element and change the action name of the rule. The new value will be parsed to check that it is valid.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/irule_list.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/irule_list.png)

### The toolbar ###

  * _Working mode_ : There are three working mode that you can select with the toolbar. The first one is the **select mode**, it is used to select a shape (microservice or arrow). This mode is only used to move shapes. The second  mode is the **arrow** mode, used to create sequences of microservices. It creates a plain arrow between microservices. The third mode is the **dotted** mode. It creates a dotted arrow between a workflow shape and a microservice shape to create the hierarchy of microservices sequences.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/work_mode.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/work_mode.png)

  * _Delete_ : This tool deletes the selected rule.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/deleteRule.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/deleteRule.png)


  * _New microservice_ : This tool adds a new microservice to the canvas. This microservice will not be attached to the parsed tree of the irule. As long as it is not linked to the displayed workflow the save will not work.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/new.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/new.png)

  * _Microservice Wizard_ and _Rule Wizard_: These tools launch wizard dialogs that helps the user to add a valid microservic or a rule call in the workflow.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/wizards.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/wizards.png)


  * _Delete_ : This tool deletes the selected shape or arrow. When a microservice is deleted, the linked arrow are also deleted. When something is deleted, the parsed tree is modified accordingly and some shapes does not belong to the model anymore, this will trigger an error if you try to save the rule.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/delete.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/delete.png)

  * _Align shapes_ : After some creation/suppression this tool reapplies the alignment algorithm to order the shapes correctly.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/align.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/align.png)

  * _Display Session variables_ : Display the session variables which are initialized for the current rule.


### The canvas ###

  * The canvas is the central component of the application, where a workflow is drawn. **You can switch between workflow and recovery with the two options in the Edit menu**.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/irule_canvas.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/irule_canvas.png)

  * The rule shape summarizes the four parts of an irule. It is updated when a modification is done on an element of the irule.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/ruleShape.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/ruleShape.png)

  * The shape for microservices is composed of an MsiShape and a MsiWorkflowShape. The MsiShape presents the name and the list of parameters. The MsiWokflowShape is represented by a square next to a parameter of the microservice, it is used to show or close a subworkflow parameter (for instance the msiDelayExec defines two subworkflows).

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/msiShape.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/msiShape.png)

  * A plain black arrow between two microservices represents a sequence of microservice (the `##` operator in an irule).

  * A red dotted arrow between a microservice and a workflow shape represents the "father" relation between the worfklow shape and the lower microservice.

### Modifying a microservice ###

  * The right panel can be used to modify the selected microservice shape. Any modification will be reflected on the rule in the canvas and in the rule shape.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/modify_msi.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/modify_msi.png)

  * The text control on the top is used to modify the name of the microservices.

  * The list below represents the parameters, it's an editable list where you can click on an element to change the value.

  * The two arrows next to the list can be used to modify the order of the parameters.

  * With the contextual menu you can add or delete a parameter.

  * The option "Set as Workflow" transform a string parameter to a MsiWorkflowShape. This specific shape can be linked to a lower subworkflow with the dotted line tool.

  * If the name of the microservice is recognized, some operations like removing a parameter may be grayed.


### Modifying the condition ###

  * The text control for the condition is parsed when you press enter in the control. If the parser fails to parse the condition the parsed tree is not modified.

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/condition.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/condition.png)


### Using Microservice Wizard ###

> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/msWizard.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/msWizard.png)

  * You can select a microservice name with the tree control on the left or with the list box on the right.

  * Once selected, additional information will be displayed.

  * Double clicking on a parameter displays the parameter dialog that show specific information on that parameter (allowed types, values).


### Using Rule Wizard ###


> ![http://irodspython.googlecode.com/svn/trunk/wiki/img/ruleWizard.png](http://irodspython.googlecode.com/svn/trunk/wiki/img/ruleWizard.png)

  * You can select a rule name with the list box on the top.

  * Once selected, additional information will be displayed.

  * Double clicking on a parameter displays the parameter dialog that show specific information on that parameter.
# iRODS Python

The iRODS python project provides a set of components to access an iRODS system with Python scripts.


## Python tools for the iRODS data grid

This project provides a set of tools that can be used to design client and server workflows for the iRODS data grid server. It consists of different subprojects, PyRods, embedPython and PyRodsAdminSet. PyRods is a client API that can be used to create client applications in Python which can communicate with a running iRODS system. embedPython is an iRODS module which allows the creation of server-side workflows written in Python. PyRods Admin is a set of GUIs that can be used to simplify iRODS management. The project has been developed as an independent software product at the University of Liverpool since 2010. It currently is being developed as part of the PERICLES project, developing code initially undertaken for the SHAMAN and PrestoPrime projects. The code is open source, under the LGPL license.


## PyRods

PyRods is a Python client API. It's a direct wrapping of the iRODS C API generated semi automatically using SWIG. It allows access to an iRODS server from Python scripts. The Python package also provides an object layer on top of the C wrapping, using these classes it is possible to work with abstract objects without needing to understand the lower level C API. This API has been successfully tested and integrated in Python client application (command line, WX) or embedded in web application written in Python (mod_python, WSGI). The code should work under 32/64 bits architecture and on Linux/Max OS.


## EmbedPython

EmbedPython is an iRODs module that needs to be installed in the iRODS system. It provides several microservices which can be used to call server-side workflows written in Python. These microservices can be used in any iRODS rules. Unlike a C microservice, a Python microservice can be modified dynamically without restarting the iRODS server.


## PyRods Admin

PyRods Admin provides a set of different administration interfaces in Python. They are using PyRods to communicate with an iRODS server. Their objective is to simplify the management of an iRODS server by proposing easy to use GUIs.
Requirements

The code has been tested under 32/64 bits architectures on Ubuntu/Mint/Fedora systems. It's also possible to build the client API under MacOsX. The latest client API is designed for the iRODS 3.2 datagrid server.

The Python API has been developped for Python 2.7. You will need the Python headers to compile the API (python2.7-dev for Ubuntu for instance).


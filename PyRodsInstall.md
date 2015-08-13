# Installation #

  * Expand the package:
> > _$ tar xvzf PyRods-3.2.3.tar.gz_

  * For a 64 bits OS add the -fPIC option to the compiler:
> > _$ export CFLAGS=-fPIC_

  * Configure the C client API:
> > _$ ./scripts/configure_

  * Build the C client API:
> > _$ make clients_

  * Build the Python library:
> > _$ python setup.py build_

  * Optionally, you can install the libraries in your python site-packages. You will need the permission to write in that directory.
> > _$ sudo python setup.py install_

  * The setup.py defines an `irods-dir` option that can be used to link to an existing iRODS install.
> > _$ python setup.py build --irods-dir=/home/rods/iRODS_


# Specific Information #

  * The library can be used under MacOsX or with GSI support. There are several Readme files in the package that explain the specific steps to follow to install the library.
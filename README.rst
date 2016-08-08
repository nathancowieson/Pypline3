Pypline3
--------

INSTALLATION
------------
Recommended to install with: pip install pypline3. The code is has a dependency on the library nexusformat which in turn depends on h5py and h5py requires that an hdf5 library with headers be on the system path. This dependency may cause the installation to fail. The hdf5 source code can be downloaded from https://www.hdfgroup.org/downloads/index.html. To do a local installation unpack, cd into the code directory and run:
./configure --prefix=/path/to/my/local/directory
make
make install

You can then install h5py using pip by setting an environmental variable pointing to your local hdf5 installation:
HDF5_DIR=/path/to/my/local/directory pip install h5py

With this dependecy taken care of you can complete the installation with:
pip setup.py install

TESTS
-----


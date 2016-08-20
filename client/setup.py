# -*- coding: utf-8 -*-

# Run the build process by running the command 'python setup.py build'
#
# If everything works well you should find a subdirectory in the build
# subdirectory that contains the files needed to run the script without Python
#
# You probably need to follow these steps:
# pip install --upgrade setuptools
# 	Install Visual C++ BuildTools if necessary: http://landinghub.visualstudio.com/visual-cpp-build-tools
# pip install cx_Freeze
# python setup.py build

from cx_Freeze import setup, Executable

executables = [
    Executable("TCE-RelayClient.py")
]

setup(name='TCE-RelayClient',
      version='0.1',
      description='TCE Relay Client',
      executables=executables
      )
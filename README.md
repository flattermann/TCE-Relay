# TCE-Relay
Updates prices in TCE from EDDB

1. What is it
TCE-Relay is a helper tool for the TCE MK II, the Trading Computer Extension for Elite Dangerous.
https://forums.frontier.co.uk/showthread.php/223056-RELEASE-Trade-Computer-Extension-Mk-II

It's a utility to update your TCE market prices to the EDDB values.
You can update all known stations at once and only older prices will be overwritten.

2. Installation
Extract ZIP to a directory of your choice (recommended: c:\TCE\TCE-Relay)

3. Usage
Call TCE-RelayClient.exe or start through TCE Launcher
If you start from command line, you'll get more debug output

4. Command Line arguments
--tce-path		path to TCE (defaults to c:/TCE)
--maxAge		Max age for the prices in days
--from-tce		is set by TCE if ran from TCE launcher

5. Compile from source
- Install Python 3.4 from http://python.org (Or a newer version, but make sure that cx_Freeze and the other modules supports your Python version!)

Open a command console (cmd.exe) and run:
	# Upgrade setuptools to latest version
	pip install --upgrade setuptools
	!! Install Visual C++ BuildTools if necessary: http://landinghub.visualstudio.com/visual-cpp-build-tools
	pip install cx_Freeze
	pip install requests
	cd <yourSourceDirectory>
	# Create .exe
	python setup.py build

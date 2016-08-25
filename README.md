# TCE-Relay
Updates prices in TCE from EDDB/EDDN

### What is it
TCE-Relay is a helper tool for the TCE MK II, the Trading Computer Extension for Elite Dangerous.

https://forums.frontier.co.uk/showthread.php/223056-RELEASE-Trade-Computer-Extension-Mk-II

It's a utility to update your TCE market prices to the EDDB/EDDN values.
You can update all known stations at once and only older prices will be overwritten.

### Installation
Extract ZIP to a directory of your choice (recommended: c:\TCE\TCE-Relay)

### Usage
Call TCE-RelayClient.exe or start through TCE Launcher.

If you start from command line, you'll get more debug output.

### Command Line arguments
```
--help, -h		show all command line parameters
--tce-path		path to TCE (defaults to c:/TCE)
--max-age		Max age for the prices in days
--from-tce		is set by TCE if ran from TCE launcher
--version, -v		show version and exit
```

### Compile from source
Install Python **3.4** from http://python.org (or a newer version, but make sure that cx_Freeze and the other modules supports your Python version!)

Open a command console (cmd.exe) and run:
```
	# Upgrade setuptools to latest version
	pip install --upgrade setuptools
	!! Install Visual C++ BuildTools if necessary: http://landinghub.visualstudio.com/visual-cpp-build-tools
	pip install cx_Freeze
	pip install requests
	cd <yourPythonDir>
	python.exe scripts\cxfreeze-postinstall
	cd <yourSourceDirectory>
	# Create .exe
	python.exe setup.py build
```

### License
TCE-Relay is licensed GPL3.

### Improvements
This is actually my **first Python script ever**. There's probably a lot of room for code improvements and performance tweaks.

Feel free to improve the code and make a pull request. I'm happy to include your optimizations!
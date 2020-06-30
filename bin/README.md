# Content

This folder contains examples of how to setup and run a fetcher (but not as a Python package, because it's not done, just as a script).


* `setup.sh` performs the following:
  * Clones the repository from GitHub
  * Creates a directory for credentials for Google sheets
  * Creates (or updates) the `conda` environment.

* `run.sh` performs the following:
  * Updates the repository (reporting if anything new was fetched)
  * Runs the fetch script for states
  * Pushes the result of the fetch to Google spreadsheet (in this example, the file to push and the target sheet is defined by command line arguments, but can be moved to a config file)

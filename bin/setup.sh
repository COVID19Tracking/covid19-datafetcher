#!/bin/sh

# clone repo
echo "Cloning repository"
git clone https://github.com/COVID19Tracking/covid19-datafetcher

# verify credentials
echo "Verifying credentials (path)"
mkdir -p creds
echo "Verify that service key file location"

cd covid19-datafetcher

# create conda env
echo "Creating (or updating) conda environment"
env_exists=$(conda env list | grep -P "^c19-data\s")

if [ -z "$env_exists" ] ; then
    conda env create -f environment.yml
else
    conda env update -f environment.yml
fi

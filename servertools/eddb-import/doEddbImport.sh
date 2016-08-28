#!/bin/bash
cd /opt/eddb-import
rm listings.csv
wget -nv https://eddb.io/archive/v4/listings.csv && python eddb-import.py

#!/bin/bash
cd /opt/eddb-import
rm listings.csv
wget -nv https://eddb.io/archive/v4/listings.csv && python import_commodity_prices.py

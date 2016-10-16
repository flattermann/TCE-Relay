#!/bin/bash
cd /opt/eddb-import

rm listings.csv
rm bodies.json

wget -nv https://eddb.io/archive/v5/listings.csv && python eddb-import-prices.py
wget -nv https://eddb.io/archive/v5/bodies.json && python eddb-import-stars.py

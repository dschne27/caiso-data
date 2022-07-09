#!/bin/bash

#Extraction 
/Users/danielschneider/opt/anaconda3/bin/python3 /Users/danielschneider/DataEng/caiso/get.py >> /Users/danielschneider/DataEng/caiso/logs.txt

FILE=/Users/danielschneider/DataEng/caiso/data/stats-$(date +%F).csv

/usr/local/bin/aws s3 cp $FILE s3://dschne-bucket/energy_data/

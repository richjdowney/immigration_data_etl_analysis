#!/bin/bash

aws s3 cp s3://immigration-data-etl/bootstrap/requirements.txt .
sudo python3 -m pip install -r requirements.txt
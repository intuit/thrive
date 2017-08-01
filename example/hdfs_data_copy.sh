#!/bin/bash

# create directories
hadoop fs -mkdir -p /thrive_sample_data/d_20170330-1800 /thrive_sample_data/d_20170330-1810

# load sample data
hadoop fs -put thrive_test_samp.txt /thrive_sample_data/d_20170330-1800/thrive_test_samp.txt
hadoop fs -put thrive_test_samp2.txt /thrive_sample_data/d_20170330-1810/thrive_test_samp2.txt


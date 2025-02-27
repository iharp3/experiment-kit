#!/bin/bash

hostname="cs-u-spatial-514.cs.umn.edu"


# # Iterate through the years
# for ((i=2014; i<=2023; i++)); do
#     echo "Copying $i"
#     scp 2m_temperature-${i}.nc $hostname:/data
#     echo "Done"
# done


# Copy all files
scp /data/era5/agg/2m_temperature/*.nc $hostname:/data
#!/bin/bash
#
# Loads the raw daily sensor data from luftdaten for all dates between the first and the second input date into
# data/raw/<YYYY-MM-DD>/ and compresses the csv files using gzip.
#
d=$1
while [ "$d" != "$2" ]; do
  echo $d
  wget --directory-prefix=data/raw --recursive --no-parent --no-host-directories --accept csv https://archive.luftdaten.info/"$d"/
  gzip data/raw/"$d"/*.csv
  d=$(date -I -d "$d + 1 day")
done

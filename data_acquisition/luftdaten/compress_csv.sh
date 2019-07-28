#!/bin/bash
#
# Iterates over all directories data/raw/<YYYY-MM-DD>/, where the date is between the first and the
# second input date and compresses the contained .csv files using gzip.
#
d=$1
while [ "$d" != "$2" ]; do
  echo $d
  gzip data/raw/"$d"/*.csv
  d=$(date -I -d "$d + 1 day")
done

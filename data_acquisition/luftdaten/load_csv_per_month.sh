#!/bin/bash
#
# Loads the zip-compressed monthly sensor data from luftdaten for all months between the first and the second input date into
# data/raw/csv_per_month/<YYYY-MM>/.
#
d=$1
while [ "$d" != "$2" ]; do
  month=$(date -ud "$d" +%Y-%m)
  echo $month
  wget --directory-prefix=data/raw --recursive --no-parent --no-host-directories --accept zip https://archive.luftdaten.info/csv_per_month/"$month"/
  d=$(date -I -d "$d + 1 months")
done

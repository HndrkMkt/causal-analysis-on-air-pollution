#!/bin/bash
#
# Loads the monthly .parquet files for each sensor type from luftdaten for all months between the first and the second input date into
# data/raw/parquet/<YYYY-MM>/<sensor_type>/.
#
d=$1
while [ "$d" != "$2" ]; do
  month=$(date -ud "$d" +%Y-%m)
  wget --directory-prefix=data/raw --recursive --no-parent --no-host-directories --accept parquet https://archive.luftdaten.info/parquet/"$month"/
  d=$(date -I -d "$d + 1 months")
done

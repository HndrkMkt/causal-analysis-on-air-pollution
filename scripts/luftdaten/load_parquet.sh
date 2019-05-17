#!/bin/bash
d=$1
while [ "$d" != "$2" ]; do
  month=$(date -ud "$d" +%Y-%m)
  wget --directory-prefix=data/raw --recursive --no-parent --no-host-directories --accept parquet https://archive.luftdaten.info/parquet/"$month"/
  d=$(date -I -d "$d + 1 months")
done

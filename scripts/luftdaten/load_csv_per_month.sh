#!/bin/bash
d=$1
while [ "$d" != "$2" ]; do
  month=$(date -ud "$d" +%Y-%m)
  echo $month
  wget --directory-prefix=data/raw --recursive --no-parent --no-host-directories --accept zip https://archive.luftdaten.info/csv_per_month/"$month"/
  d=$(date -I -d "$d + 1 months")
done

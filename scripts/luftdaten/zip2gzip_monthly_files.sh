#!/bin/bash
d=$1
while [ "$d" != "$2" ]; do
  month=$(date -ud "$d" +%Y-%m)
  echo $month
  unzip "data/raw/csv_per_month/$month/*.zip" -d data/raw/csv_per_month/"$month"/
  gzip data/raw/csv_per_month/"$month"/*.csv
  rm data/raw/csv_per_month/"$month"/*.zip
  d=$(date -I -d "$d + 1 months")
done

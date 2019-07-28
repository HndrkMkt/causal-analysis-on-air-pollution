#!/bin/bash
#
# Changes the compression for the zip-compressed monthly sensor data in data/raw/csv_per_month/<YYYY-MM>/ between the
# first and the second input date to gzip by first uncompressing the files and then using gzip to recompress.
#
# Note: This needs to be done due to Flink not being able to handle .zip files as input.
#
d=$1
while [ "$d" != "$2" ]; do
  month=$(date -ud "$d" +%Y-%m)
  echo $month
  unzip "data/raw/csv_per_month/$month/*.zip" -d data/raw/csv_per_month/"$month"/
  gzip data/raw/csv_per_month/"$month"/*.csv
  rm data/raw/csv_per_month/"$month"/*.zip
  d=$(date -I -d "$d + 1 months")
done

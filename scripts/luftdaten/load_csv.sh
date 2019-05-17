#!/bin/bash
d=$1
while [ "$d" != "$2" ]; do
  echo $d
  wget --directory-prefix=data/raw --recursive --no-parent --no-host-directories --accept csv https://archive.luftdaten.info/"$d"/
  gzip data/raw/"$d"/*.csv
  d=$(date -I -d "$d + 1 day")
done

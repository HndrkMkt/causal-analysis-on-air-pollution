#!/bin/bash
d=$1
while [ "$d" != "$2" ]; do
  echo $d
  gzip data/raw/"$d"/*.csv
  d=$(date -I -d "$d + 1 day")
done

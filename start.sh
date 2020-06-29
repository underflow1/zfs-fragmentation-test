#!/bin/bash
for i in 1 .. 3
do
echo -e "Destroing old pool"
zpool destroy poola && echo " pool destroyed"
sleep 1
echo -e "Creating new pool"
zpool create poola -o ashift=12 /dev/sda4 && echo " pool created" || exit
sleep 2
echo -e "Setting recordsize"

zfs set recordsize=2M poola && echo " size is set"
sleep 1
echo -e "Turning on compression"
zfs set compression=zle poola && echo " compression is on"
sleep 1
echo -e "Starting test cycle"
python3 start.py poola
done

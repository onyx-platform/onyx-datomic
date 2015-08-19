#!/bin/sh
IP=$(/sbin/ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}')
echo $IP > eth0.ip
docker run -d -p 4334-4336:4334-4336 -e ALT_HOST=$IP --name datomic-free akiel/datomic-free

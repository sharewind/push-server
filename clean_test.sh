#!/bin/sh
set -x

#clean mongodb 
mongo 10.10.69.191:27017/push -eval "db.devices.remove(); db.messages.remove(); db.subs.remove()"
mongo 10.10.69.191:27018/push -eval "db.devices.remove(); db.messages.remove(); db.subs.remove()"

#clean redis
echo "flushdb\r\n" | nc 10.10.79.123 15151


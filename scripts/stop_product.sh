#!/bin/sh
api_pid=`ps aux|grep "/push/bin/api"|grep -v "grep"|awk '{print $2}'`
if [ -n api_pid ]
then 
	echo "api pid  is {$api_pid}"
	kill -9 $api_pid
	echo "api $api_pid is killed!"
fi

broker_pid=`ps aux|grep "/push/bin/broker"|grep -v "grep"|awk '{print $2}'`
if [ -n broker_pid ]
then 
	echo "broker pid  is {$broker_pid}"
	kill -9 $broker_pid
	echo "broker $broker_pid is killed!"
fi

# worker_pid=`ps aux|grep "/push/bin/worker"|grep -v "grep"|awk '{print $2}'`
# if [ -n worker_pid ]
# then 
# 	echo "worker pid  is {$worker_pid}"
# 	kill -9 $worker_pid
# 	echo "worker $worker_pid is killed!"
# fi

#!/bin/bash
set -x
target_ips="10.10.79.134"
target_dir="/opt/webapps/push"
webapp="push"

sh build.sh


for ip in $target_ips
do 
	echo "=================== start deploy on $ip ======================="
	ssh root@$ip " rm -rf $target_dir"
	scp dist/$webapp.tar.gz root@$ip:/opt/webapps/
	ssh root@$ip " tar -zxf /opt/webapps/$webapp.tar.gz -C /opt/webapps/"

	ssh root@$ip " sh $target_dir/scripts/stop_dev.sh"
	ssh root@$ip " sh $target_dir/scripts/start_dev.sh"
done

echo "======================= finish ==================================="


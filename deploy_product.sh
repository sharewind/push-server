#!/bin/bash
set -x
target_ips="192.168.105.136"
target_dir="/opt/webapps/push"
webapp="push"

sh build.sh


for ip in $target_ips
do 
	echo "=================== start deploy on $ip ======================="
	ssh root@$ip " rm -rf $target_dir"
	scp dist/$webapp.tar.gz root@$ip:/opt/webapps/
	ssh root@$ip " tar -zxf /opt/webapps/$webapp.tar.gz -C /opt/webapps/"

	ssh root@$ip " sh $target_dir/scripts/stop_product.sh"
	ssh root@$ip " sh $target_dir/scripts/start_product.sh"
done

echo "======================= finish ==================================="


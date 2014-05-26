scp dist/push.tar.gz cjf@10.2.58.177:~/

su
ps aux|grep push/bin/broker|grep -v grep|awk '{print $2}'|xargs kill -9
echo "flushdb\r\n" | nc -C 10.10.79.123 15151
rm -rfv /home/cjf/push
tar -zxvf push.tar.gz
sh push/scripts/setup_sysctl.sh
nohup push/bin/broker --tcp-address=0.0.0.0:8600 --http-address=0.0.0.0:8601  --broadcast-address=10.2.58.177:8600 &> broker.log &


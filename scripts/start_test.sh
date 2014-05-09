ulimit -n 1048576
mkdir -pv /opt/logs/push/
nohup /opt/webapps/push/bin/api --http-address="0.0.0.0:8501" --broker-tcp-address="192.168.105.136:8600" &> /opt/logs/push/api.log  &
nohup /opt/webapps/push/bin/broker --broker-tcp-address="192.168.105.136:8600" --broadcast-address="192.168.105.136:8600" --worker-http-address="0.0.0.0:8710" &> /opt/logs/push/broker.log &

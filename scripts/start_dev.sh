ulimit -n 1048576
mkdir -pv /opt/logs/push/
nohup /opt/webapps/push/bin/api --http-address="0.0.0.0:8501" --broker-tcp-address="10.10.79.134:8600" &> /opt/logs/push/api.log  &
nohup /opt/webapps/push/bin/broker --tcp-address="0.0.0.0:8600" --broadcast-address="10.10.79.134:8600"  &> /opt/logs/push/broker.log &
nohup /opt/webapps/push/bin/worker --http-address="0.0.0.0:8710" --broker-tcp-address="10.10.79.134:8600"  &> /opt/logs/push/worker.log  &

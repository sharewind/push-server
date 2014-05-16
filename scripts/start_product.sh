ulimit -n 1048576
mkdir -pv /opt/logs/push/

export REDIS_SERVER="10.11.157.178:15261"
export MONGO_URL="mongodb://192.168.230.52:27017,192.168.230.53:27017,192.168.230.54:27017?connect=replicaSet"
nohup /opt/webapps/push/bin/api --http-address="0.0.0.0:80" --broker-tcp-address="123.125.116.56:8600" &> /opt/logs/push/api.log  &
nohup /opt/webapps/push/bin/broker --tcp-address="0.0.0.0:8600" --http-address="10.11.157.18:8601" --broadcast-address="10.11.157.18:8600"  &> /opt/logs/push/broker.log &

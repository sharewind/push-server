ulimit -n 1048576
mkdir -pv /opt/logs/push/

export REDIS_SERVER="10.11.157.178:15261"
export MONGO_URL="mongodb://192.168.230.52:27017,192.168.230.53:27017,192.168.230.54:27017?connect=replicaSet"
nohup /opt/webapps/push/bin/api --http-address="123.125.116.56:80" --broker-tcp-address="123.125.116.57:80" &> /opt/logs/push/api.log  &

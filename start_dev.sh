nohup ./api --http-address="0.0.0.0:4171" &> /opt/logs/api.log  &
nohup ./broker --tcp-address="0.0.0.0:8600" --broadcast-address="10.10.79.134:8600"  &> /opt/logs/broker.log &
nohup ./worker --htcp-address="0.0.0.0:8710" --broker-tcp-address="10.10.79.134:8600"  &> /opt/logs/worker.log  &

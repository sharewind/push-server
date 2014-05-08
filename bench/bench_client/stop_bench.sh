ps aux|grep bench_client|grep -v grep|awk '{print $2}'|xargs kill -9 

ps aux|grep bench|grep -v grep|awk '{print }'|xargs kill -9 

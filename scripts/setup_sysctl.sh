
#tcp
sysctl -w net.ipv4.tcp_tw_reuse=1
sysctl -w net.ipv4.tcp_fin_timeout=15
sysctl -w net.ipv4.tcp_rmem="4096 87380 16777216"
sysctl -w net.ipv4.tcp_wmem="4096 65536 16777216"
sysctl -w net.core.netdev_max_backlog=102400
sysctl -w net.core.somaxconn=40960
sysctl -w net.ipv4.tcp_max_syn_backlog=20480

#max open file
sysctl -w fs.file-max=1048576
ulimit -n 1048576





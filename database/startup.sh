iptables -P INPUT DROP
iptables -P OUTPUT DROP
iptables -P FORWARD DROP
for ip in 73.229.29.2 172.16.0.3
do
  iptables -A INPUT -s $ip -p tcp --dport 27017 -j ACCEPT
  iptables -A OUTPUT -d $ip -p tcp -j ACCEPT
done
iptables -L
mongod --bind_ip 127.0.0.1,172.16.0.2

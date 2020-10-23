FROM mongo:4.4
RUN apt-get update
RUN apt-get install apt-utils -y
RUN apt-get install netfilter-persistent -y
RUN mkdir -p /etc/iptables
RUN service netfilter-persistent start

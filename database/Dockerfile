FROM mongo:4.4
RUN apt-get update
RUN apt-get install apt-utils -y
RUN apt-get install netfilter-persistent -y
RUN mkdir -p /etc/iptables
COPY rules.v4 /etc/iptables/rules.v4
RUN service netfilter-persistent start

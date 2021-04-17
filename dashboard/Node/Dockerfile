FROM node
WORKDIR /Workspace
RUN git clone https://github.com/JamesOsgood/mongodb-grafana.git
WORKDIR /Workspace/mongodb-grafana
RUN npm install
CMD npm run server

#!/bin/bash

if ! wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | sudo apt-key add -; then
    sudo apt-get install gnupg
    if ! wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | sudo apt-key add -; then
        err "Cannot import key"
        exit 1
    fi
fi
echo "Public key imported"

echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list

sudo apt-get update

sudo apt-get install -y mongodb-org
echo "MongoDB successfully installed"

if ! sudo systemctl start mongod; then
    sudo systemctl daemon-reload
fi
echo "MongoDB started"

sudo systemctl enable mongod
#!/bin/sh

yum -y install golang

yum install git

mkdir -p /home/golang

echo 'export GOROOT=/usr/lib/golang
export GOBIN=$GOROOT/bin
export GOPATH=/home/golang
export PATH=$PATH:$GOROOT/bin:$GOPATH/bin' > /etc/profile.d/go.sh

echo '# Golang Path
export GOROOT=/usr/lib/golang
export GOBIN=$GOROOT/bin
export GOPATH=/home/golang
export PATH=$PATH:$GOROOT/bin$GOPATH/bin' >> ~/.bashrc

source ~/.bashrc

source /etc/profile

ldconfig

cd /home/golang && git clone https://github.com/vkuznet/transfer2go

cd /home/golang/transfer2go && go get && go build
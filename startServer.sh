#!/bin/bash
app="path-finding"
docker build -t ${app} .
# docker run -d -p 56733:80 \
#   --name=${app} \
#   -v $PWD:/app ${app}


docker run -d -p 127.0.0.1:25000:5656 path-finding
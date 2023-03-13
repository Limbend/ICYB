#!/bin/bash
docker build -t limbend/icyb:v1 .
docker stop icyb
docker rm icyb
docker run -it --name='icyb' -e 'ICYB_L_TYPE'='RELEASE' --net=host --restart unless-stopped limbend/icyb:v1

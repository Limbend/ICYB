#!/bin/bash
docker build -t limbend/icyb:v1 .
docker stop icyb
docker rm icyb
docker run -it --name='icyb' -e 'ICYB_L_TYPE'='RELEASE' --net=host limbend/icyb:v1

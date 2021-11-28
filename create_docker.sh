#!/bin/bash
docker build -t limbend/icyb:v1 .
docker stop icyb
docker rm icyb
docker run -it --name='icyb' --net=host limbend/icyb:v1

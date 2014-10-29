FROM ubuntu:14.04
MAINTAINER Carsten Ehbrecht <ehbrecht@dkrz.de>

RUN apt-get update && apt-get install -y git wget
WORKDIR /tmp
RUN wget https://raw.githubusercontent.com/bird-house/malleefowl/pingudev/requirements.sh
RUN bash requirements.sh
RUN useradd -d /home/phoenix -m phoenix
USER phoenix
RUN git clone -b pingudev --single-branch https://github.com/bird-house/malleefowl.git 
RUN cd malleefowl && bash bootstrap.sh && cd -
RUN cd malleefowl && bash install.sh build && cd -
WORKDIR /home/phoenix/anaconda

EXPOSE 8080 8090 8091 9001

CMD bin/supervisord -n -c etc/supervisor/supervisord.conf && bin/nginx -c etc/nginx/nginx.conf -g 'daemon off;'


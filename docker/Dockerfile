FROM ubuntu:20.04

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
## essential files
RUN apt-get update  
RUN apt-get install -y git gcc make cmake libssh-dev 
RUN apt-get install -y wget unzip zip openssh-server 
RUN apt-get clean 

RUN wget https://golang.org/dl/go1.16.linux-amd64.tar.gz 
## install golang and set environment 
RUN tar -C /usr/local -xzf go1.16.linux-amd64.tar.gz
ENV PATH "$PATH:/usr/local/go/bin"

# set the working directory
WORKDIR /work

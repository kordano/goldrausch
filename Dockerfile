FROM ubuntu:trusty

# Update the APT cache
RUN apt-get   update

# Install and setup project dependencies
RUN apt-get install -y curl git wget unzip

# prepare for Java download
RUN apt-get install -y software-properties-common
RUN apt-get -y install openjdk-7-jre-headless
RUN apt-get install -y mongodb-clients
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

# fix wget
RUN export HTTP_CLIENT="wget --no-check-certificate -O"

# grab leiningen
RUN wget https://raw.github.com/technomancy/leiningen/stable/bin/lein -O /usr/local/bin/lein
RUN chmod +x /usr/local/bin/lein
ENV LEIN_ROOT yes
RUN lein

# grab datomic
RUN wget https://my.datomic.com/downloads/free/0.9.4899 -O /opt/datomic.zip
RUN unzip /opt/datomic.zip -d /opt
RUN mv /opt/datomic-free-0.9.4899 /opt/datomic

# add scripts
ADD ./opt /opt

# grab project
RUN git clone https://github.com/kordano/goldrausch.git /opt/goldrausch

# define port
EXPOSE 8091

CMD ["/opt/start-it"]

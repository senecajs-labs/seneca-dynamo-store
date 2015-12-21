# Use phusion/baseimage as base image. Using latest for now.
# https://github.com/phusion/baseimage-docker
FROM phusion/baseimage:latest

# Install Java, node and npm
RUN \
  apt-get update && \
  apt-get install -y \
  curl \
  openjdk-7-jre \
  nodejs \
  npm


# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64


# Link nodejs binary
RUN ln -s /usr/bin/nodejs /usr/bin/node


# Install dev dynamodb
WORKDIR /tmp
RUN mkdir -p /usr/local/dynamodb/data \
    && curl -SL http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz \
    | tar -xvzC /usr/local/dynamodb


# Run DynamoDB as a service
RUN mkdir /etc/service/dynamodb
RUN echo '#!/bin/sh\n' > /etc/service/dynamodb/run
RUN echo 'java -Djava.library.path=/usr/local/dynamodb/DynamoDBLocal_lib -jar /usr/local/dynamodb/DynamoDBLocal.jar -sharedDb -dbPath /usr/local/dynamodb/data' >> /etc/service/dynamodb/run
RUN chmod +x /etc/service/dynamodb/run


# Copy testable code
WORKDIR /code
COPY ./lib /code/lib
COPY ./script /code/script
COPY ./test /code/test
COPY ./package.json /code/


# Install Node deps
RUN npm install

# Use baseimage-docker's init system in order to start dynamo service,
# then npm test command is run
CMD ["/sbin/my_init", "--", "npm", "test"]

# ...put your own build instructions here...

# Clean up APT when done.
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

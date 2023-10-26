#      #######      #         #       ##########          #            #########
#      #            # #     # #       #        #         # #           #        #
#      #            #  #   #  #       #        #        #   #          #         #
#      #######      #   # #   #       ##########       #######         #          #
#      #            #    #    #       #               #       #        #         #
#      #            #         #       #              #         #       #        #
#      ######       #         #       #             #           #      #########
#
# version 1.6
# @author: Amir H. Sharifzadeh, The Institute of Data Intensive Engineering and Science, Johns Hopkins University
# @since: 06/14/2023
# @last modified: 07/15/2023

FROM python:3.9-slim
#
#RUN apt-get update
#RUN apt-get install -y apt-utils build-essential gcc
#
#ENV JAVA_FOLDER java-se-8u41-ri
#
#ENV JVM_ROOT /usr/lib/jvm
#
#ENV JAVA_PKG_NAME openjdk-8u41-b04-linux-x64-14_jan_2020.tar.gz
#ENV JAVA_TAR_GZ_URL https://download.java.net/openjdk/jdk8u41/ri/$JAVA_PKG_NAME

FROM maven:3.8.1-openjdk-11 AS build
RUN mkdir -p /app/src
COPY src /app/src
COPY pom.xml /app

RUN mvn -f /app/pom.xml clean package

FROM openjdk:11-jre-slim
FROM flink:1.17.0

RUN apt-get update
RUN apt-get install -y python3 python3-pip
RUN rm -rf /var/lib/apt/lists/*

RUN apt-get clean
RUN apt-get autoremove
RUN pip3 install numpy

COPY --from=build /app/target/varimat-stream-processing-1.6.jar /opt/flink/lib/varimat-stream-processing-1.6.jar
COPY --from=build /app/target/varimat-stream-processing-1.6-jar-with-dependencies.jar /opt/flink/lib/varimat-stream-processing-1.6-jar-with-dependencies.jar

RUN mkdir -p /empad/mask
COPY mask/mask.mat /empad/mask

RUN mkdir -p /empad/means
COPY means /empad/means

RUN mkdir -p /empad/temp
COPY temp /empad/temp

RUN mkdir -p /empad/temp/prc
COPY output /empad/temp/prc

RUN mkdir -p /empad/temp/out
COPY output /empad/temp/out

RUN mkdir -p /empad/histogram
COPY output /empad/histogram

RUN mkdir -p /empad/output
COPY output /empad/output

RUN mkdir -p /empad/pt
COPY pt/unpacker_out.py /empad/pt

COPY --from=build /app/target/dependency-jars /opt/flink/lib/dependency-jars

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "-Xms48g", "-Xmx60g" ,"/opt/flink/lib/varimat-stream-processing-1.6.jar"]

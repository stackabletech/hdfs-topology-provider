#
# mvn clean install
# docker build ./target -t docker.stackable.tech/stackable/hadoop-with-topology-provider:3.3.6-stackable0.0.0-dev -f ./Dockerfile
#

FROM docker.stackable.tech/stackable/hadoop:3.3.6-stackable0.0.0-dev

COPY --chown=stackable:stackable ./hdfs-topology-provider-0.2.0-SNAPSHOT.jar /stackable/hadoop/share/hadoop/tools/lib/
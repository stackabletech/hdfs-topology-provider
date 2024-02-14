#
# mvn clean install
# VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
# docker build ./target --build-arg PROJECT_VERSION=$VERSION -t docker.stackable.tech/stackable/hadoop-with-topology-provider:3.3.6-stackable0.0.0-dev -f ./Dockerfile
#

FROM docker.stackable.tech/stackable/hadoop:3.3.6-stackable0.0.0-dev

ARG PROJECT_VERSION

COPY --chown=stackable:stackable ./hdfs-topology-provider-$PROJECT_VERSION.jar /stackable/hadoop/share/hadoop/tools/lib/
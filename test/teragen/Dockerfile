#
# cd test/stack/teragen
# docker build . -t docker.stackable.tech/stackable/spark-k8s-with-teragen:3.5.0-stackable0.0.0-dev
#

FROM docker.stackable.tech/stackable/spark-k8s:3.5.1-stackable0.0.0-dev

# this .jar is compiled from the code here: https://github.com/ehiggs/spark-terasort
COPY --chown=stackable:stackable ./spark-terasort-1.2-SNAPSHOT.jar /tmp/
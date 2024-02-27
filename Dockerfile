FROM docker.stackable.tech/stackable/hadoop:3.3.6-stackable0.0.0-dev

RUN rm -rf /stackable/hadoop/share/hadoop/tools/lib/hdfs-topology-provider-*.jar
COPY --chown=stackable:stackable ./hdfs-topology-provider-*.jar /stackable/hadoop/share/hadoop/tools/lib/

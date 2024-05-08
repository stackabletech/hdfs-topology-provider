# Stackable Network Topology Provider for Apache Hadoop

[Documentation (HDFS Operator)](https://docs.stackable.tech/home/stable/hdfs) | [Stackable Data Platform](https://stackable.tech/) | [Platform Docs](https://docs.stackable.tech/) | [Discussions](https://github.com/orgs/stackabletech/discussions) | [Discord](https://discord.gg/7kZ3BNnCAF)

Hadoop supports a concept called [rack awareness](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/RackAwareness.html) (lately also often called topology awareness).

Historically this has been used to distinguish racks, datacenters and the like and often been read from a manually updated topology file or similar solutions.

In Kubernetes, the most commonly used mechanism for topology awareness are labels - mostly labels set on the Kubernetes nodes.
The most prevalent example for this is the node label [topology.kubernetes.io/zone](https://kubernetes.io/docs/reference/labels-annotations-taints/#topologykubernetesiozone) which often refers to availability zones in cloud providers or similar things.

The purpose of this tool is to feed information from Kubernetes into the HDFS rack awareness functionality.
In order to do this, it implements the Hadoop interface `org.apache.hadoop.net.DNSToSwitchMapping` which then allows this tool to be configured on the NameNode via the parameter `net.topology.node.switch.mapping.impl`.

The topology provider watches all HDFS pods deployed by Stackable and Kubernetes nodes and keeps an in memory cache of the current state of these objects.
From this state store the tool can then calculate rack IDs for nodes that HDFS asks for without needing to talk to the api-server and incurring an extra network round-trip.

Results are cached for a configurable amount of time and served from the cache if present.

In a Kubernetes environment it is likely that the majority of writes will not come from the DataNodes themselves, but rather from other processes such as Spark executors writing data to HDFS. The NameNode passes these on to the topology provider to request the rack ID i.e. it provides the IP addresses of whichever pods are doing the writing. If a datanode resides on the same Kubernetes node as one of these pods, then this datanode is used for label resolution for that pod.

## Configuration

Configuration of the tool happens via environment variables, as shown below:

### TOPOLOGY_LABELS

A semicolon separated list of labels that should be used to build a rack id for a datanode.

A label is specified as `[node|pod]:<labelname>`

Some examples:

|            Definition            |                                             Resolved to                                              |
|----------------------------------|------------------------------------------------------------------------------------------------------|
| node:topology.kubernetes.io/zone | The value of the label 'topology.kubernetes.io/zone' on the node to which the pod has been assigned. |
| pod:app.kubernetes.io/role-group | The value of the label 'app.kubernetes.io/role-group' on the datanode pod.                           |

Multiple levels of labels can be combined (up to MAX_TOPOLOGY_LABELS levels) by separating them with a semicolon:

So for example `node:topology.kubernetes.io/zone;pod:app.kubernetes.io/role-group` would resolve to `/<value of label topology.kubernetes.io/zone on the node>/<value of label app.kubernetes.io/role-group on the pod>`.

### TOPOLOGY_CACHE_EXPIRATION_SECONDS

Default: 5 Minutes

The default time for which rack ids are cached, HDFS can influence caching, as the provider offers methods to invalidate the cache, so this is not a totally _reliable_ value, as there are external factors not under the control of this tool.

### MAX_TOPOLOGY_LEVELS

Default: 2

The maximum number of levels that can be specified to build a rack id from.

While this can be changed, HDFS probably only supports a maximum of two levels, so it is not recommended to change the default for this setting.

## Testing

There are currently no unit tests.

CRDs for spinning up test infrastructure are provided in `test/stack`.

The actual testing for this happens in the integration tests of the HDFS operator.

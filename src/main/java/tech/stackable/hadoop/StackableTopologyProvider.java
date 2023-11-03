package tech.stackable.hadoop;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodIP;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * An implementation of the org.apache.hadoop.net.DNSToSwitchMapping that is used to create a topology out of
 * datanodes.
 * <p>
 * This class is intended to be run as part of the NameNode process and will be used by the namenode to retrieve
 * topology strings for datanodes.
 */
public class StackableTopologyProvider implements DNSToSwitchMapping {

    private static final int MAX_LEVELS_DEFAULT = 2;

    private class TopologyLabel {
        LabelType labelType;
        String name = null;

        public boolean isUndefined() {
            return this.labelType == LabelType.Undefined;
        }

        /**
         * Create a new TopologyLabel from its string representation
         *
         * @param value A string in the form of "[node|pod]:<labelname>" that is deserialized into a TopologyLabel.
         *              Invalid and empty strings are resolved into the type unspecified.
         */
        public TopologyLabel(String value) {
            // If this is null the env var was not set, we will return 'undefined' for this level
            if (value == null || value.equals("")) {
                this.labelType = LabelType.Undefined;
                return;
            }
            String[] split = value.toLowerCase(Locale.ROOT).split(":", 2);

            // This should only fail if no : was present in the string
            if (split.length != 2) {
                this.labelType = LabelType.Undefined;
                LOG.warn("Ignoring topology label [" + value + "] - label definitions need to be in the form of \"[node|pod]:<labelname>\"");
                return;
            }
            // Length has to be two, proceed with "normal" case
            String type = split[0];
            this.name = split[1];

            // Parse type of object labels should be retrieved from
            switch (type) {
                case "node":
                    this.labelType = LabelType.Node;
                    break;

                case "pod":
                    this.labelType = LabelType.Pod;
                    break;

                default:
                    LOG.warn("Encountered unsupported labeltype [" + type + "] - this label definition will be ignored, supported types are [\"node\", \"pod\"]");
                    this.labelType = LabelType.Undefined;
            }
        }
    }

    private enum LabelType {
        Node,
        Pod,
        Undefined
    }

    private KubernetesClient client;

    private Cache<String, String> topologyKeyCache = Caffeine.newBuilder()
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build();


    // The list of labels that this provider uses to generate the topologyinformation for any given datanode
    private List<TopologyLabel> labels;

    private final Logger LOG = LoggerFactory.getLogger(StackableTopologyProvider.class);

    public StackableTopologyProvider() {
        this.client = new DefaultKubernetesClient();

        // Read the labels to be used to build a topology from environment variables
        // Labels are configured in the EnvVar "TOPOLOGY_LABELS".
        // They should be specified in the form "[node|pod]:<labelname>" and separated by ;
        // So a valid configuration that reads topologyinformation from the labels "kubernetes.io/zone" and
        // "kubernetes.io/rack" on the k8s node that is running a datanode pod would look like this:
        // "node:kubernetes.io/zone;node:kubernetes.io/rack"
        //
        // By default, there is an upper limit of 2 on the number of labels that are processed, because this is what
        // Hadoop traditionally allows - this can be overridden via setting the EnvVar "MAX_TOPOLOGY_LEVELS".
        String topologyConfig = System.getenv("TOPOLOGY_LABELS");
        if (topologyConfig != null && topologyConfig != "") {
            String[] labelConfigs = topologyConfig.split(";");
            if (labelConfigs.length > getMaxLabels()) {
                LOG.error("Found [" + labelConfigs.length + "] topologylabels configured, but maximum allowed number is " + getMaxLabels() + "please check your config or raise the number of allowed labels.");
                throw new RuntimeException();
            }
            // Create TopologyLabels from config strings
            this.labels = Arrays.stream(labelConfigs).map(labelConfig -> {
                return new TopologyLabel(labelConfig);
            }).collect(Collectors.toList());

            // Check if any labelConfigs were invalid
            if (!this.labels.stream().filter(label -> {
                return label.labelType == LabelType.Undefined;
            }).collect(Collectors.toList()).isEmpty()) {
                LOG.error("Topologylabel contained invalid configuration for at least one label, please double check your config!");
                throw new RuntimeException();
            }

        } else {
            LOG.error("Mising env var \"TOPOLOGYLABELS\" this is required for rack awareness to work.");
            throw new RuntimeException();
        }
    }

    private int getMaxLabels() {
        String envConfig = System.getenv("MAX_TOPOLOGY_LEVELS");
        if (envConfig != null && envConfig != "") {
            LOG.info("Found MAX_TOPOLOGY_LEVELS env var, changing allowed number of topology levels.");
            try {
                int maxLevelsFromEnvVar = Integer.parseInt(envConfig);
            } catch (NumberFormatException e) {
                LOG.warn("Unable to parse MAX_TOPOLOGY_LEVELS as integer, using default value: " + e.getLocalizedMessage());
            }
        }
        return MAX_LEVELS_DEFAULT;
    }

    private String getLabel(String datanode, Map<String, Map<String, String>> podLabels, Map<String, Map<String, String>> nodeLabels) {
        String result = new String();

        // The internal structures used by this mapper are all based on IP addresses. Depending on configuration and network setup
        // it may (probably will) be possible that the namenode uses hostnames to resolve a datanode to a topology zone.
        // To allow this, we resolve every input to an ip address below and use the ip to lookup labels.
        // TODO: this might break with the listener operator, as `pod.status.podips` won't contain external addresses
        //      tracking this in
        InetAddress address = null;
        try {
            address = InetAddress.getByName(datanode);
            LOG.debug("Resolved [" + datanode + "] to [" + address.getHostAddress() + "]");
            datanode = address.getHostAddress();
        } catch (UnknownHostException e) {
            LOG.warn("failed to resolve address for [" + datanode + "] - this should not happen, defaulting this node to \"defaultRack\".");
            return ("/defaultRack");
        }
        for (TopologyLabel label : this.labels) {
            if (label.labelType == LabelType.Node) {
                result += "/" + nodeLabels.getOrDefault(datanode, new HashMap<>()).getOrDefault(label.name, "NotFound");
            } else if (label.labelType == LabelType.Pod) {
                result += "/" + podLabels.getOrDefault(datanode, new HashMap<>()).getOrDefault(label.name, "NotFound");
            }
        }
        return result;
    }

    public List<String> resolve(List<String> names) {
        LOG.debug("Resolving nodes: " + names.toString());

        if (this.labels.size() == 0) {
            LOG.warn("No topology labels defined, returning \"/defaultrack\" for all nodes.");
            return names.stream().map(name -> "/defaultRack").collect(Collectors.toList());
        }

        // We need to check if we have cached values for all datanodes contained in this request.
        // Unless we can answer everything from the cache we have to talk to k8s anyway and can just
        // recalculate everything
        List<String> cachedValues = names.stream().map(name -> {
            return this.topologyKeyCache.getIfPresent(name);
        }).collect(Collectors.toList());

        if (cachedValues.contains(null)) {
            // We cannot completely serve this request from the cache, since we need to talk to k8s anyway
            // we'll simply refresh everything.
            LOG.debug("Cache doesn't contain values for all requested nodes, new values will be built for all nodes.");
        } else {
            LOG.debug("Answering from cached topology keys.");
            return cachedValues;
        }

        // Retrieve all datanode pods. This should be restricted to the current namespace by the serviceaccount
        // we roll out in our helm charts anyway, so it is not further contained here.
        List<Pod> pods = client
                .pods()
                .withLabel("app.kubernetes.io/component", "datanode")
                .withLabel("app.kubernetes.io/name", "hdfs")
                .list()
                .getItems();
        LOG.debug("Retrieved dn pods: " + pods.stream().map(pod -> {
            return pod.getMetadata().getName();
        }).collect(Collectors.toList()));

        List<Node> nodes = client.nodes().list().getItems();
        LOG.debug("Retrieved nodes: " + pods.stream().map(node -> {
            return node.getMetadata().getName();
        }).collect(Collectors.toList()));

        // Build internal state that is later used to look up information.
        // Basically this transposes pod and node lists into hashmaps where podips can be used to look up
        // labels for the pods and nodes
        // This is not terribly memory efficient because it effectively duplicates a lot of data in memory.
        // But since we cache lookups, this should hopefully only be done every once in a while and is not kept
        // in memory for extended amounts of time.
        List<String> result = new ArrayList<>();
        Map<String, Map<String, String>> nodeLabels = getNodeLabels(pods, nodes);
        LOG.debug("Resolved nodelabels for: " + nodeLabels.keySet().toString());
        Map<String, Map<String, String>> podLabels = getPodLabels(pods);
        LOG.debug("Resolved podlabels for " + podLabels.keySet().toString());

        // Iterate over all nodes to resolve and return the topology zones
        for (String datanode : names) {
            String builtLabel = getLabel(datanode, podLabels, nodeLabels);
            result.add(builtLabel);

            // Cache the value for potential use in a later request
            this.topologyKeyCache.put(datanode, builtLabel);
        }
        return result;
    }


    private Map<String, Map<String, String>> getPodLabels(List<Pod> pods) {
        Map<String, Map<String, String>> result = new HashMap<>();
        for (Pod pod : pods) {
            for (PodIP podIp : pod.getStatus().getPodIPs()) {
                result.put(podIp.getIp(), pod.getMetadata().getLabels());
            }
        }
        return result;
    }

    private Map<String, Map<String, String>> getNodeLabels(List<Pod> pods, List<Node> nodes) {
        Map<String, Map<String, String>> result = new HashMap<>();
        this.LOG.warn(nodes.toString());
        for (Pod pod : pods) {
            Map<String, String> nodeLabels = nodes.stream().filter(filterNode -> {
                return filterNode.getMetadata().getName().equals(pod.getSpec().getNodeName());
            }).collect(Collectors.toList()).get(0).getMetadata().getLabels();

            for (PodIP podIp : pod.getStatus().getPodIPs()) {
                result.put(podIp.getIp(), nodeLabels);
            }
        }
        this.LOG.warn(result.toString());
        return result;
    }

    public void reloadCachedMappings() {
        this.topologyKeyCache.invalidateAll();

    }

    public void reloadCachedMappings(List<String> names) {
        for (String name : names) {
            this.topologyKeyCache.invalidate(name);
        }
    }
}

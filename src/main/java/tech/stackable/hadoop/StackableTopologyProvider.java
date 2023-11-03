package tech.stackable.hadoop;

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
         * @param value A string in the form of "[node|pod]:<labelname>" that is deserialized into a TopologyLabel.
         *              Invalid and empty strings are resolved into the type unspecified.
         */
        public TopologyLabel(String value) {
            // If this is null the env var was not set, we will return 'undefined' for this level
            if (value == null || value.equals("")) {
                this.labelType = LabelType.Undefined;
                return;
            }
            String[] split = value.toLowerCase(Locale.ROOT).split( ":", 2);

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

    // The list of labels that this provider uses to generate the topologyinformation for any given datanode
    private List<TopologyLabel> labels;

    private final Logger LOG = LoggerFactory.getLogger(StackableTopologyProvider.class);

    public StackableTopologyProvider() {
        this.client = new DefaultKubernetesClient();

        // Read the labels to be used to build a topology from environment variables
        // A fixed prefix of 'TOPOLOGYLABEL' is used for these vars, and the code simply increases a counter
        // that is added to this prefix, starting with 1 - upon encountering the first empty label iteration is
        // aborted.
        // By default, there is an upper limit of 2 on the number of labels that are processed, because this is what
        // Hadoop traditionally allows - this can be overridden via setting the EnvVar "MAX_TOPOLOGY_LEVELS".
        this.labels = new ArrayList<>();
        for (int i = 1; i <= getMaxLabels(); i++) {
            TopologyLabel potentialLabel = new TopologyLabel(System.getenv("TOPOLOGYLABEL" + i));
            if (potentialLabel.isUndefined()) {
                // We break upon encountering the first undefined label
                LOG.info("Reached end of defined topology labels, found [" + this.labels.size() + "] labels.");
                break;
            } else {
                LOG.info("Adding topology-label [" + potentialLabel + "]");
                this.labels.add(potentialLabel);
            }
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


        List<Pod> pods = client.pods().list().getItems();
        LOG.warn("Retrieved pods: " + pods.stream().map(pod -> {
            return pod.getMetadata().getName();
        }).collect(Collectors.toList()));
        List<Node> nodes = client.nodes().list().getItems();
        LOG.warn("Retrieved nodes: " + pods.stream().map(node -> {
            return node.getMetadata().getName();
        }).collect(Collectors.toList()));

        List<String> result = new ArrayList<>();
        Map<String, Map<String, String>> nodeLabels = getNodeLabels(pods, nodes);
        LOG.warn("Resolved nodelabels for: " + nodeLabels.keySet().toString());
        Map<String, Map<String, String>> podLabels = getPodLabels(pods);
        LOG.warn("Resolved podlabels for " + podLabels.keySet().toString());

        for (String datanode : names) {
            result.add(getLabel(datanode, podLabels, nodeLabels));
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

    }

    public void reloadCachedMappings(List<String> names) {

    }
}

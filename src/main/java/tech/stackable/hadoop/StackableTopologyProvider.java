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
 * Hello world!
 */
public class StackableTopologyProvider implements DNSToSwitchMapping {

    private static final int MAX_LEVELS = 2;

    private class TopologyLabel {
        LabelType labelType;
        String name = null;

        public boolean isUndefined() {
            return this.labelType == LabelType.Undefined;
        }

        public TopologyLabel(String value) {
            // If this is null the env var was not set, we will return 'undefined' for this level
            // TODO: should probably return empty, but we need to make sure to return a default if no label is set
            if (value == null || value.equals("")) {
                this.labelType = LabelType.Undefined;
                return;
            }
            String[] split = value.toLowerCase(Locale.ROOT).split(":");
            if (split.length != 2) {
                this.labelType = LabelType.Undefined;
                LOG.warn("Ignoring topology label [" + value + "] - label definitions need to be in the form of \"[node|pod]:<labelname>\"");
                return;
            }
            // Length has to be two, proceed with "normal" case
            String type = split[0];
            this.name = split[1];

            switch (type) {
                case "node":
                    this.labelType = LabelType.Node;
                    break;

                case "pod":
                    this.labelType = LabelType.Pod;
                    break;

                default:
                    this.labelType = LabelType.Undefined;
            }
        }
    }

    private enum LabelType {
        Node,
        Pod,
        Undefined
    }

    //public class StackableTopologyProvider {
    private KubernetesClient client;
    private List<TopologyLabel> labels;
    private TopologyLabel levelOneLabel;
    private TopologyLabel levelTwoLabel;
    private final Logger LOG = LoggerFactory.getLogger(StackableTopologyProvider.class);

    public StackableTopologyProvider() {
        this.client = new DefaultKubernetesClient();
        this.labels = new ArrayList<>();
        for (int i = 1; i <= MAX_LEVELS; i++) {
            TopologyLabel potentialLabel = new TopologyLabel(System.getenv("TOPOLOGYLABEL" + i));
            if (potentialLabel.isUndefined()) {
                // We break upon encountering the first undefined label
                LOG.info("Reached end of defined topology labels, found [" + this.labels.size() + "] labels.");
                break;
            } else {
                LOG.info("Adding topology-label [" + potentialLabel +"]");
                this.labels.add(potentialLabel);
            }
        }
        this.levelOneLabel = new TopologyLabel(System.getenv("TOPOLOGYLABEL1"));
        this.levelTwoLabel = new TopologyLabel("TOPOLOGYLABEL2");
    }

    private String getValue(String name, TopologyLabel label, Map<String, Map<String, String>> podLabels, Map<String, Map<String, String>> nodeLabels) {
        if (label.labelType == LabelType.Node) {
            return "/" + nodeLabels.getOrDefault(name, new HashMap<>()).getOrDefault(label.name, "nodemissing");
        } else if (label.labelType == LabelType.Pod) {
            return "/" + podLabels.getOrDefault(name, new HashMap<>()).getOrDefault(label.name, "podmissing");
        } else {
            return "/undefined";
        }
    }

    private String getLabel(String datanode, Map<String, Map<String, String>> podLabels, Map<String, Map<String, String>> nodeLabels) {
        String result = new String();
        InetAddress address = null;
        try {
            address = InetAddress.getByName(datanode);
            LOG.warn("Resolved [" + datanode + "] to [" +  address.getHostAddress() + "]");
            datanode = address.getHostAddress();
        } catch (UnknownHostException e) {
            LOG.warn("failed to resolve address for [" + datanode + "]");
            return("/defaultRack");
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
        LOG.warn("Retrieved pods: " + pods.stream().map(pod -> {return pod.getMetadata().getName();}).collect(Collectors.toList()));
        List<Node> nodes = client.nodes().list().getItems();
        LOG.warn("Retrieved nodes: " + pods.stream().map(node -> {return node.getMetadata().getName();}).collect(Collectors.toList()));

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

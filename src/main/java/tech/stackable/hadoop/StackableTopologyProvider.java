package tech.stackable.hadoop;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodIP;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class StackableTopologyProvider implements DNSToSwitchMapping {

    private class TopologyLabel {
        LabelType labelType;
        String name = null;

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
    private TopologyLabel levelOneLabel;
    private TopologyLabel levelTwoLabel;
    private final Logger LOG = LoggerFactory.getLogger(StackableTopologyProvider.class);

    public StackableTopologyProvider() {
        this.client = new DefaultKubernetesClient();
        this.levelOneLabel = new TopologyLabel(System.getenv("TOPOLOGYLABEL1"));
        this.levelTwoLabel = new TopologyLabel("TOPOLOGYLABEL2");
    }

    private String getValue(String name, TopologyLabel label, Map<String, Map<String, String>> podLabels, Map<String, Map<String, String>> nodeLabels) {
        if (label.labelType == LabelType.Node) {
            return nodeLabels.getOrDefault(name, new HashMap<>()).getOrDefault(label.name, "/nodemissing");
        } else if (label.labelType == LabelType.Pod) {
            return podLabels.getOrDefault(name, new HashMap<>()).getOrDefault(label.name, "/podmissing");
        } else {
            return "/undefined";
        }
    }

    public List<String> resolve(List<String> names) {
        if (this.isUndefined()) {
            return names.stream().map(name -> "/defaultRack").collect(Collectors.toList());
        }

        List<Pod> pods = client.pods().list().getItems();
        List<Node> nodes = client.nodes().list().getItems();

        List<String> result = new ArrayList<>();
        Map<String, Map<String, String>> nodeLabels = getNodeLabels(pods, nodes);
        Map<String, Map<String, String>> podLabels = getPodLabels(pods);

        for (String datanode : names) {
            String part1 = getValue(datanode, this.levelOneLabel, podLabels, nodeLabels);
            String part2 = getValue(datanode, this.levelTwoLabel, podLabels, nodeLabels);
            result.add(part1.concat(part2));
        }
        return result;
    }

    private boolean isUndefined() {
        return this.levelOneLabel.labelType == LabelType.Undefined && this.levelTwoLabel.labelType == LabelType.Undefined;
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

package tech.stackable.hadoop;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the org.apache.hadoop.net.DNSToSwitchMapping that is used to create a
 * topology out of datanodes.
 *
 * <p>This class is intended to be run as part of the NameNode process and will be used by the
 * namenode to retrieve topology strings for datanodes.
 */
public class StackableTopologyProvider implements DNSToSwitchMapping {

  public static final String VARNAME_LABELS = "TOPOLOGY_LABELS";
  public static final String VARNAME_CACHE_EXPIRATION = "TOPOLOGY_CACHE_EXPIRATION_SECONDS";
  public static final String VARNAME_MAXLEVELS = "TOPOLOGY_MAX_LEVELS";
  public static final String DEFAULT_RACK = "/defaultRack";
  private static final int MAX_LEVELS_DEFAULT = 2;
  private static final int CACHE_EXPIRY_DEFAULT_SECONDS = 5 * 60;
  private final Logger LOG = LoggerFactory.getLogger(StackableTopologyProvider.class);
  private final KubernetesClient client;
  private final Cache<String, String> topologyKeyCache =
      Caffeine.newBuilder().expireAfterWrite(getCacheExpiration(), TimeUnit.SECONDS).build();
  private final Cache<String, Node> nodeKeyCache =
      Caffeine.newBuilder()
          .expireAfterWrite(CACHE_EXPIRY_DEFAULT_SECONDS, TimeUnit.SECONDS)
          .build();

  private final Cache<String, GenericKubernetesResource> listenerKeyCache =
      Caffeine.newBuilder()
          .expireAfterWrite(CACHE_EXPIRY_DEFAULT_SECONDS, TimeUnit.SECONDS)
          .build();
  private final Cache<String, Pod> podKeyCache =
      Caffeine.newBuilder()
          .expireAfterWrite(CACHE_EXPIRY_DEFAULT_SECONDS, TimeUnit.SECONDS)
          .build();
  // The list of labels that this provider uses to generate the topology information for any given
  // datanode
  private final List<TopologyLabel> labels;

  public StackableTopologyProvider() {
    this.client = new KubernetesClientBuilder().build();

    // Read the labels to be used to build a topology from environment variables. Labels are
    // configured in the EnvVar "TOPOLOGY_LABELS". They should be specified in the form
    // "[node|pod]:<labelname>" and separated by ";". So a valid configuration that reads topology
    // information from the labels "kubernetes.io/zone" and "kubernetes.io/rack" on the k8s node
    // that is running a datanode pod would look like this:
    // "node:kubernetes.io/zone;node:kubernetes.io/rack" By default, there is an upper limit of 2 on
    // the number of labels that are processed, because this is what Hadoop traditionally allows -
    // this can be overridden via setting the EnvVar "MAX_TOPOLOGY_LEVELS".
    String topologyConfig = System.getenv(VARNAME_LABELS);
    if (topologyConfig != null && !topologyConfig.isEmpty()) {
      String[] labelConfigs = topologyConfig.split(";");
      if (labelConfigs.length > getMaxLabels()) {
        LOG.error(
            "Found [{}] topology labels configured, but maximum allowed number is [{}]: "
                + "please check your config or raise the number of allowed labels.",
            labelConfigs.length,
            getMaxLabels());
        throw new RuntimeException();
      }
      // Create TopologyLabels from config strings
      this.labels =
          Arrays.stream(labelConfigs).map(TopologyLabel::new).collect(Collectors.toList());

      // Check if any labelConfigs were invalid
      if (this.labels.stream().anyMatch(label -> label.labelType == LabelType.Undefined)) {
        LOG.error(
            "Topologylabel contained invalid configuration for at least one label: "
                + "double check your config! Labels should be specified in the "
                + "format '[pod|node]:<labelname>;...'");
        throw new RuntimeException();
      }

    } else {
      LOG.error(
          "Missing env var [{}] this is required for rack awareness to work.", VARNAME_LABELS);
      throw new RuntimeException();
    }
    if (this.labels.isEmpty()) {
      LOG.info(
          "No topology config found, defaulting value for all datanodes to [{}]", DEFAULT_RACK);
    } else {
      LOG.info(
          "Topology config yields labels [{}]",
          this.labels.stream().map(label -> label.name).collect(Collectors.toList()));
    }
  }

  /***
   * Checks if a value for the maximum number of topology levels to allow has been configured in
   * the environment variable specified in VARNAME_MAXLEVELS,
   * returns the value of MAX_LEVELS_DEFAULT as default if nothing is set.
   *
   * @return The maximum number of topology levels to allow.
   */
  private int getMaxLabels() {
    String maxLevelsConfig = System.getenv(VARNAME_MAXLEVELS);
    if (maxLevelsConfig != null && !maxLevelsConfig.isEmpty()) {
      try {
        int maxLevelsFromEnvVar = Integer.parseInt(maxLevelsConfig);
        LOG.info(
            "Found [{}] env var, changing allowed number of topology levels to [{}]",
            VARNAME_MAXLEVELS,
            maxLevelsFromEnvVar);
        return maxLevelsFromEnvVar;
      } catch (NumberFormatException e) {
        LOG.warn(
            "Unable to parse value of [{}]/[{}] as integer, using default value [{}]",
            VARNAME_MAXLEVELS,
            maxLevelsConfig,
            MAX_LEVELS_DEFAULT);
      }
    }
    return MAX_LEVELS_DEFAULT;
  }

  /***
   * Checks if a value for the cache expiration time has been configured in
   * the environment variable specified in VARNAME_CACHE_EXPIRATION,
   * returns the value of CACHE_EXPIRY_DEFAULT_SECONDS as default if nothing is set.
   *
   * @return The cache expiration time to use for the rack id cache.
   */
  private int getCacheExpiration() {
    String cacheExpirationConfigSeconds = System.getenv(VARNAME_CACHE_EXPIRATION);
    if (cacheExpirationConfigSeconds != null && !cacheExpirationConfigSeconds.isEmpty()) {
      try {
        int cacheExpirationFromEnvVar = Integer.parseInt(cacheExpirationConfigSeconds);
        LOG.info(
            "Found [{}] env var, changing cache time for topology entries to [{}]",
            VARNAME_CACHE_EXPIRATION,
            cacheExpirationFromEnvVar);
        return cacheExpirationFromEnvVar;
      } catch (NumberFormatException e) {
        LOG.warn(
            "Unable to parse value of [{}]/[{}] as integer, using default value [{}]",
            VARNAME_CACHE_EXPIRATION,
            cacheExpirationConfigSeconds,
            CACHE_EXPIRY_DEFAULT_SECONDS);
      }
    }
    return CACHE_EXPIRY_DEFAULT_SECONDS;
  }

  /***
   *
   * @param datanode the datanode whose IP mis to be resolved
   * @param podLabels the pod labels used in the resolution
   * @param nodeLabels the node labels used in the resolution
   *
   * @return the label looked up from the IP address
   */
  private String getLabel(
      String datanode,
      Map<String, Map<String, String>> podLabels,
      Map<String, Map<String, String>> nodeLabels) {
    // The internal structures used by this mapper are all based on IP addresses. Depending on
    // configuration and network setup it may (probably will) be possible that the namenode uses
    // hostnames to resolve a datanode to a topology zone. To allow this, we resolve every input to
    // an ip address below and use the ip to lookup labels.
    // TODO: this might break with the listener operator, as `pod.status.podips` won't contain
    // external addresses
    //      tracking this in https://github.com/stackabletech/hdfs-topology-provider/issues/2
    InetAddress address;
    try {
      address = InetAddress.getByName(datanode);
      LOG.debug("Resolved [{}] to [{}]", datanode, address.getHostAddress());
      datanode = address.getHostAddress();
    } catch (UnknownHostException e) {
      LOG.warn(
          "failed to resolve address for [{}] - this should not happen, "
              + "defaulting this node to [{}]",
          datanode,
          DEFAULT_RACK);
      return DEFAULT_RACK;
    }
    StringBuilder resultBuilder = new StringBuilder();
    for (TopologyLabel label : this.labels) {
      if (label.labelType == LabelType.Node) {
        LOG.debug(
            "Looking for node label [{}] of type [{}] in [{}]/[{}]",
            label.name,
            label.labelType,
            nodeLabels.keySet(),
            nodeLabels.values());
        resultBuilder
            .append("/")
            .append(
                nodeLabels
                    .getOrDefault(datanode, new HashMap<>())
                    .getOrDefault(label.name, "NotFound"));
      } else if (label.labelType == LabelType.Pod) {
        LOG.debug(
            "Looking for pod label [{}] of type [{}] in [{}]/[{}]",
            label.name,
            label.labelType,
            podLabels.keySet(),
            podLabels.values());
        resultBuilder
            .append("/")
            .append(
                podLabels
                    .getOrDefault(datanode, new HashMap<>())
                    .getOrDefault(label.name, "NotFound"));
      }
    }
    String result = resultBuilder.toString();
    LOG.debug("Returning label [{}]", result);
    return result;
  }

  @Override
  public List<String> resolve(List<String> names) {
    LOG.info("Resolving for listeners/client-pods [{}]", names.toString());

    if (this.labels.isEmpty()) {
      LOG.info(
          "No topology labels defined, returning [{}] for hdfs nodes: [{}]", DEFAULT_RACK, names);
      return names.stream().map(name -> DEFAULT_RACK).collect(Collectors.toList());
    }

    // We need to check if we have cached values for all datanodes contained in this request.
    // Unless we can answer everything from the cache we have to talk to k8s anyway and can just
    // recalculate everything
    List<String> cachedValues =
        names.stream().map(this.topologyKeyCache::getIfPresent).collect(Collectors.toList());
    LOG.debug("Cached topologyKeyCache values [{}]", cachedValues);

    if (cachedValues.contains(null)) {
      // We cannot completely serve this request from the cache, since we need to talk to k8s anyway
      // we'll simply refresh everything.
      LOG.debug(
          "Cache doesn't contain values for all requested pods: new values will be built for all nodes.");
    } else {
      // use same log level as the non-cached return statement
      LOG.info("Answering from cached topology keys: [{}]", cachedValues);
      return cachedValues;
    }

    // The datanodes will be the cache keys.
    List<Pod> datanodes =
        client
            .pods()
            .withLabel("app.kubernetes.io/component", "datanode")
            .withLabel("app.kubernetes.io/name", "hdfs")
            .list()
            .getItems();
    LOG.debug(
        "Retrieved datanodes: [{}]",
        datanodes.stream()
            .map(datanode -> datanode.getMetadata().getName())
            .collect(Collectors.toList()));

    List<String> namesToDataNodeNames = dataNodesResolvedFromListenerOrOriginal(names);
    LOG.debug("Now resolving: [{}]", namesToDataNodeNames);

    // Build internal state that is later used to look up information. Basically this transposes pod
    // and node lists into hashmaps where pod-IPs can be used to look up labels for the pods and
    // nodes. This is not terribly memory efficient because it effectively duplicates a lot of data
    // in memory. But since we cache lookups, this should hopefully only be done every once in a
    // while and is not kept in memory for extended amounts of time.
    List<String> result = new ArrayList<>();

    Map<String, Map<String, String>> nodeLabels = getNodeLabels(datanodes);
    LOG.debug("Resolved node labels map [{}]/[{}]", nodeLabels.keySet(), nodeLabels.values());

    Map<String, Map<String, String>> podLabels = getPodLabels(datanodes);
    LOG.debug("Resolved pod labels map [{}]/[{}]", podLabels.keySet(), podLabels.values());

    List<String> podsResolvedToDataNodes =
        resolveDataNodesFromCallingPods(namesToDataNodeNames, podLabels, datanodes);

    // Iterate over all nodes to resolve and return the topology zones
    for (int i = 0; i < podsResolvedToDataNodes.size(); i++) {
      String builtLabel = getLabel(podsResolvedToDataNodes.get(i), podLabels, nodeLabels);
      result.add(builtLabel);

      // Cache the value for potential use in a later request
      this.topologyKeyCache.put(podsResolvedToDataNodes.get(i), builtLabel);
      // also cache the original name, in case that has resolved to a dataNode (so that
      // the resolution step can be omitted next time this pod is encountered)
      this.topologyKeyCache.put(names.get(i), builtLabel);
    }
    LOG.info("Returning resolved labels [{}]", result);
    return result;
  }

  /**
   * If the names include listeners then these must be resolved against the dataNode IPs and used
   * subsequently.
   *
   * @param names the collection of names to resolve
   * @return a collection of either the name (for non-listener) or the dataNode IP to which this
   *     listener resolves
   */
  private List<String> dataNodesResolvedFromListenerOrOriginal(List<String> names) {
    List<GenericKubernetesResource> cachedListeners =
        names.stream().map(this.listenerKeyCache::getIfPresent).collect(Collectors.toList());
    if (cachedListeners.contains(null)) {
      LOG.debug(
          "Refreshing listener cache as not all of [{}] present in [{}]",
          names,
          this.listenerKeyCache.asMap().keySet());

      GenericKubernetesResourceList listeners = getListeners();

      for (GenericKubernetesResource listener : listeners.getItems()) {
        this.listenerKeyCache.put(listener.getMetadata().getName(), listener);
        // also add the IPs
        for (String ingressAddress : TopologyUtils.getIngressAddresses(listener)) {
          this.listenerKeyCache.put(ingressAddress, listener);
        }
      }
    } else {
      LOG.debug("Listener cache contains [{}]", names);
    }
    ConcurrentMap<String, GenericKubernetesResource> listeners = this.listenerKeyCache.asMap();

    List<String> listenerToDataNodeNames = new ArrayList<>();

    for (String name : names) {
      String resolvedName = resolveDataNodesFromListeners(name, listeners);
      listenerToDataNodeNames.add(resolvedName);
    }
    return listenerToDataNodeNames;
  }

  private GenericKubernetesResourceList getListeners() {
    ResourceDefinitionContext listenerCrd =
        new ResourceDefinitionContext.Builder()
            .withGroup("listeners.stackable.tech")
            .withVersion("v1alpha1")
            .withPlural("listeners")
            .withNamespaced(true)
            .build();

    return client.genericKubernetesResources(listenerCrd).list();
  }

  /**
   * We don't know if the name refers to a listener (it could be any client pod) but we check to see
   * if it can be resolved to a dataNode just in case.
   *
   * @param name the name of the calling pod which should be resolved to a dataNode IP if it is a
   *     listener
   * @param listeners the current listener collection
   * @return either the name (for non-listener) or the dataNode IP to which this listener resolves
   */
  private String resolveDataNodesFromListeners(
      String name, ConcurrentMap<String, GenericKubernetesResource> listeners) {
    LOG.debug("Attempting to resolve [{}]", name);
    for (GenericKubernetesResource listener : listeners.values()) {
      List<String> ingressAddresses = TopologyUtils.getIngressAddresses(listener);
      for (String ingressAddress : ingressAddresses) {
        LOG.debug("Address [{}]", ingressAddress);
        if (name.equalsIgnoreCase(ingressAddress)) {
          LOG.debug("Matched ingressAddress [{}]/[{}]", name, listener.getMetadata().getName());

          Endpoints ep = client.endpoints().withName(listener.getMetadata().getName()).get();
          // TODO: Assuming single address per datanode endpoint.
          // When does/can an endpoint support multiple data nodes? On restart the address list will
          // be empty for a moment or two.
          if (ep.getSubsets().size() < 1) {
            LOG.warn(
                "Endpoint [{}] address not detected, pod maybe restarting...",
                ep.getMetadata().getName());
          } else {
            EndpointAddress address = ep.getSubsets().get(0).getAddresses().get(0);
            LOG.info(
                "Endpoint [{}], IP [{}], node [{}]",
                ep.getMetadata().getName(),
                address.getIp(),
                address.getNodeName());
            return address.getIp();
          }
        }
      }
    }
    LOG.info("Not a listener, returning [{}]", name);
    return name;
  }

  /**
   * The list of names may be datanodes (as is the case when the topology is initialised) or
   * non-datanodes, when the data is being written by a client tool, spark executors etc. In this
   * case we want to identify the datanodes that are running on the same node as this client. The
   * names may also be pod IPs or pod names.
   *
   * @param names list of client pods to resolve to datanodes
   * @param podLabels map of podIPs and labels
   * @param dns list of datanode names which will be used to match nodenames
   * @return a list of pods resolved to co-located datanodes where possible
   */
  private List<String> resolveDataNodesFromCallingPods(
      List<String> names, Map<String, Map<String, String>> podLabels, List<Pod> dns) {
    List<String> dataNodes = new ArrayList<>();

    // if any of the names do not exist in the pod cache then refresh it
    List<Pod> cachedPods =
        names.stream().map(this.podKeyCache::getIfPresent).collect(Collectors.toList());
    if (cachedPods.contains(null)) {
      LOG.info(
          "Refreshing pod cache as not all of [{}] present in [{}]",
          names,
          this.podKeyCache.asMap().keySet());
      for (Pod pod : client.pods().list().getItems()) {
        this.podKeyCache.put(pod.getMetadata().getName(), pod);
        // also add an entry for each IP
        for (PodIP ip : pod.getStatus().getPodIPs()) {
          this.podKeyCache.put(ip.getIp(), pod);
        }
      }
    } else {
      LOG.info("Pod cache contains [{}]", names);
    }
    ConcurrentMap<String, Pod> pods = this.podKeyCache.asMap();

    for (String name : names) {
      // if we don't find a dataNode running on the same node as a non-dataNode pod, then
      // we'll keep the original name to allow it to be resolved to NotFound in the calling routine.
      String replacementDataNodeIp = name;
      InetAddress address;
      try {
        // make sure we are looking up using the IP address
        address = InetAddress.getByName(name);
        String podIp = address.getHostAddress();
        if (podLabels.containsKey(podIp)) {
          replacementDataNodeIp = podIp;
          LOG.info("Added as found in the datanode map [{}]", podIp);
        } else {
          // we've received a call from a non-datanode pod
          for (Pod pod : pods.values()) {
            if (pod.getStatus().getPodIPs().contains(new PodIP(podIp))) {
              String nodeName = pod.getSpec().getNodeName();
              for (Pod dn : dns) {
                if (dn.getSpec().getNodeName().equals(nodeName)) {
                  LOG.debug(
                      "NodeName [{}] matches with [{}]?", dn.getSpec().getNodeName(), nodeName);
                  replacementDataNodeIp = dn.getStatus().getPodIP();
                  break;
                }
              }
            }
          }
        }
      } catch (UnknownHostException e) {
        LOG.warn("Error encountered while resolving host [{}]", e.getLocalizedMessage());
      }
      dataNodes.add(replacementDataNodeIp);
    }
    LOG.info("Replacing names [{}] with IPs [{}]", names, dataNodes);
    return dataNodes;
  }

  /**
   * Given a list of datanodes, return a HashMap that maps pod ips onto Pod labels. The returned Map
   * may contain more entries than the list that is given to this function, as an entry will be
   * generated for every ip a pod has.
   *
   * @param datanodes List of all retrieved pods.
   * @return Map of ip addresses to all labels the pod that "owns" that ip has attached to itself
   */
  private Map<String, Map<String, String>> getPodLabels(List<Pod> datanodes) {
    Map<String, Map<String, String>> result = new HashMap<>();
    // Iterate over all pods and then all ips for every pod and add these to the mapping
    for (Pod pod : datanodes) {
      for (PodIP podIp : pod.getStatus().getPodIPs()) {
        result.put(podIp.getIp(), pod.getMetadata().getLabels());
      }
    }
    return result;
  }

  /**
   * Given a list of datanodes this function will resolve which datanodes run on which node as well
   * as all the ips assigned to a datanodes. It will then return a mapping of every ip address to
   * the labels that are attached to the "physical" node running the datanodes that this ip belongs
   * to.
   *
   * @param datanodes List of all in-scope datanodes (datanode pods in this namespace)
   * @return Map of ip addresses to labels of the node running the pod that the ip address belongs
   *     to
   */
  private Map<String, Map<String, String>> getNodeLabels(List<Pod> datanodes) {
    Map<String, Map<String, String>> result = new HashMap<>();
    for (Pod pod : datanodes) {
      // either retrieve the node from the internal cache or fetch it by name
      String nodeName = pod.getSpec().getNodeName();
      // nodeName may be null while pod is being provisioned so force a re-try
      if (nodeName == null) {
        LOG.warn(
            "Pod [{}] not yet assigned to a k8s node, forcing re-try", pod.getMetadata().getName());
        return result;
      }
      Node node = this.nodeKeyCache.getIfPresent(nodeName);
      if (node == null) {
        LOG.debug("Node not yet cached, fetching by name [{}]", nodeName);
        node = client.nodes().withName(nodeName).get();
        this.nodeKeyCache.put(nodeName, node);
      }

      Map<String, String> nodeLabels = node.getMetadata().getLabels();
      LOG.debug("Labels for node [{}]:[{}]....", nodeName, nodeLabels);

      for (PodIP podIp : pod.getStatus().getPodIPs()) {
        LOG.debug("...assigned to IP [{}]", podIp.getIp());
        result.put(podIp.getIp(), nodeLabels);
      }
    }
    return result;
  }

  @Override
  public void reloadCachedMappings() {
    // TODO: According to the upstream comment we should rebuild all cache entries after
    // invalidating them
    //  this may mean trying to resolve ip addresses that do not exist any more and things like that
    // though and
    //  require some more thought, so we will for now just invalidate the cache.
    this.topologyKeyCache.invalidateAll();
  }

  @Override
  public void reloadCachedMappings(List<String> names) {
    // TODO: See comment above, the same applies here
    for (String name : names) {
      this.topologyKeyCache.invalidate(name);
    }
  }

  private enum LabelType {
    Node,
    Pod,
    Undefined
  }

  private class TopologyLabel {
    private final LabelType labelType;
    private String name = null;

    /**
     * Create a new TopologyLabel from its string representation
     *
     * @param value A string in the form of "[node|pod]:<labelname>" that is deserialized into a
     *     TopologyLabel. Invalid and empty strings are resolved into the type unspecified.
     */
    private TopologyLabel(String value) {
      // If this is null the env var was not set, we will return 'undefined' for this level
      if (value == null || value.isEmpty()) {
        this.labelType = LabelType.Undefined;
        return;
      }
      String[] split = value.toLowerCase(Locale.ROOT).split(":", 2);

      // This should only fail if no : was present in the string
      if (split.length != 2) {
        this.labelType = LabelType.Undefined;
        LOG.warn(
            "Ignoring topology label [{}] - label definitions need to be in the form "
                + "of \"[node|pod]:<labelname>\"",
            value);
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
          LOG.warn(
              "Encountered unsupported label type [{}] - this label definition will be ignored, "
                  + "supported types are [\"node\", \"pod\"]",
              type);
          this.labelType = LabelType.Undefined;
      }
    }
  }
}

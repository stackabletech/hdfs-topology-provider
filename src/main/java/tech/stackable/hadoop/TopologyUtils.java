package tech.stackable.hadoop;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TopologyUtils {
  private static final String ADDRESS = "address";
  private static final String STATUS = "status";
  private static final String INGRESS_ADDRESSES = "ingressAddresses";

  public static List<String> getIngressAddresses(GenericKubernetesResource listener) {
    // suppress warning as we know the structure of our own listener resource
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> ingressAddresses =
        ((List<Map<String, Object>>)
            ((Map<String, Object>) listener.getAdditionalProperties().get(STATUS))
                .get(INGRESS_ADDRESSES));
    return ingressAddresses.stream()
        .map(ingress -> (String) ingress.get(ADDRESS))
        .collect(Collectors.toList());
  }
}

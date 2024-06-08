import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONArray;
import org.json.JSONObject;

public class JsonParser {

  public JSONObject parseJsonFile(String path) throws IOException {
    try {
      String jsonString = new String(Files.readAllBytes(Paths.get(path)));
      return new JSONObject(jsonString);
    } catch (IOException e) {
      System.err.println("Error reading JSON file: " + e.getMessage());
      throw e;
    }
  }

  public HashMap<Integer, ArrayList<String>> parseEvaluationPlan(JSONObject jsonObject) {
    /* {0: ['SEQ(A, F)'], 1: [], 2: [], 3: [], 4: ['SEQ(A, F)', 'SEQ(A, B, F)']} */
    HashMap<Integer, ArrayList<String>> evaluationPlan = new HashMap<>();
    for (String node : jsonObject.keySet()) {
      evaluationPlan.put(Integer.parseInt(node), new ArrayList<>());
      JSONArray events = jsonObject.getJSONArray(node);
      System.out.println(Integer.parseInt(node));
      if (events.length() == 0) {
        continue;
      }
      for (int i = 0; i < events.length(); i++) {
        evaluationPlan.get(Integer.parseInt(node)).add(events.getString(i));
        System.out.println(Integer.parseInt(node) + " : " + events.getString(i));
      }
    }
    return evaluationPlan;
  }

  public Set<InputRules> parseForwardingRules(JSONObject jsonObject) {
    /* {'F': {'F1': {1: [2], 2: [0], 0: [4]}, 'F3': {3: [4], 4: [0]}}, 'A': {'A': {}}, SEQ(A, F): {'A0F': {0: [4]}}, 'B': {'B': {}}} */
    Set<InputRules> forwardingRules = new HashSet<>();

    for (String input : jsonObject.keySet()) {
      Integer srcNode = null;

      System.out.println("input event: " + input);
      JSONObject inputRules = jsonObject.getJSONObject(input);
      for (String inputNode : inputRules.keySet()) {
        System.out.println("input node: " + inputNode);

        JSONObject hops = inputRules.getJSONObject(inputNode);
        System.out.println("hops: " + hops.toString());
        if (hops.length() == 0) {
          System.out.println("input not forwarded");
          continue;
        }
        HashMap<Integer, ArrayList<Integer>> srcDstMap = new HashMap<>();

        int cnt = 0;
        for (String src : hops.keySet()) {
          Integer srcHop = Integer.parseInt(src);

          try {
            srcNode = Integer.parseInt(inputNode.replaceAll("[^0-9]", ""));
          } catch (NumberFormatException e) {
            if (cnt == 0) {
              srcNode = srcHop;
            }
          }
          System.out.println("src node: " + srcNode);

          System.out.println("src hop: " + srcHop);
          JSONArray dtsHopsJson = hops.getJSONArray(src);
          ArrayList<Integer> dstsHops = new ArrayList<>();
          System.out.println("dst hops: ");
          for (int i = 0; i < dtsHopsJson.length(); i++) {
            dstsHops.add(dtsHopsJson.getInt(i));
            System.out.println(dtsHopsJson.getInt(i));
          }
          System.out.println("src hops: " + srcHop + " dst hops: " + dstsHops.toString());
          srcDstMap.put(srcHop, dstsHops);
          cnt++;
        }
        if (srcNode == null) {
          System.out.println("input not forwarded");
          InputRulesNotForwarded inputRulesNotForwarded = new InputRulesNotForwarded(input, false);
          forwardingRules.add(inputRulesNotForwarded);
        } else {
          InputRulesForwarded inputRulesForwarded =
              new InputRulesForwarded(input, srcNode, true, srcDstMap);
          forwardingRules.add(inputRulesForwarded);
        }
      }
    }
    return forwardingRules;
  }

  public HashMap<String, ArrayList<String>> parseProjectionInputs(JSONObject jsonObject) {

    /* {'SEQ(A, F)': ['F', 'A'], 'SEQ(A, B, F)': ['SEQ(A, F)', 'B']} */
    HashMap<String, ArrayList<String>> projectionInputs = new HashMap<>();
    for (String projection : jsonObject.keySet()) {
      // System.out.println("projection: " + projection);
      JSONArray inputsJson = jsonObject.getJSONArray(projection);
      ArrayList<String> inputs = new ArrayList<>();
      // System.out.println("inputs: ");
      for (int i = 0; i < inputsJson.length(); i++) {
        inputs.add(inputsJson.getString(i));
        System.out.println(inputsJson.getString(i));
      }
      projectionInputs.put(projection, inputs);
    }
    return projectionInputs;
  }

  public HashMap<Integer, ArrayList<String>> parseEventAssignments(JSONObject jsonObject) {

    /* {0: ['A', 'C', 'D', 'E'], 1: ['F'], 2: ['D', 'E'], 3: ['D', 'E', 'F'], 4: ['A', 'B']} */
    HashMap<Integer, ArrayList<String>> eventAssignments = new HashMap<>();
    for (String nodeStr : jsonObject.keySet()) {
      // System.out.println("node: " + nodeStr);
      JSONArray eventTypesJson = jsonObject.getJSONArray(nodeStr);
      ArrayList<String> eventTypes = new ArrayList<>();
      // System.out.println("event types: ");
      for (int i = 0; i < eventTypesJson.length(); i++) {
        eventTypes.add(eventTypesJson.getString(i));
        // System.out.println(eventTypesJson.getString(i));
      }
      eventAssignments.put(Integer.parseInt(nodeStr), eventTypes);
    }
    return eventAssignments;
  }

  private static List<Integer> jsonArrayToInt(JSONArray jsonArray) {
    return jsonArray.toList().stream()
        .map(Object::toString)
        .map(Integer::parseInt)
        .collect(Collectors.toList());
  }

  public Set<NodeForwardingRules> parseNodeRules(JSONObject jsonObject) {

    Set<NodeForwardingRules> nodeRules = new HashSet<>();
    JSONArray ftJson = jsonObject.getJSONObject("forwarding").getJSONArray("forwarding_table");
    for (int i = 0; i < ftJson.length(); i++) {
      JSONArray ftEntry =
          ftJson.getJSONArray(
              i); // form of an entry: [event_type, list_of_sources, list_of_destinations]
      String eventType = ftEntry.getString(0);
      ArrayList<Integer> sourceNodes = (ArrayList) jsonArrayToInt(ftEntry.getJSONArray(1));
      ArrayList<Integer> destNodes = (ArrayList) jsonArrayToInt(ftEntry.getJSONArray(2));
      System.out.println("source nodes: " + sourceNodes);
      System.out.println("dest nodes: " + destNodes);
      for (Integer srcNode : sourceNodes) {
        for (Integer dstNode : destNodes) {
          System.out.println(
              "creating a node rule: " + eventType + ", " + srcNode + " -> " + dstNode);
          nodeRules.add(new NodeForwardingRules(eventType, dstNode));
        }
      }

      // nodeRules.add(new NodeForwardingRules(eventType, sourceNodes, destNodes));
    }
    System.out.println("rules created: " + nodeRules.size() + "\n");
    return nodeRules;
  }

  public ArrayList<Tuple2<Integer, Integer>> parseNetworkEdges(JSONObject jsonObject) {

    ArrayList<Tuple2<Integer, Integer>> networkEdges = new ArrayList<>();
    JSONArray edgesJson = jsonObject.getJSONArray("edges");
    System.out.println("\nNetwork edges: ");
    for (int i = 0; i < edgesJson.length(); i++) {
      JSONArray edge = edgesJson.getJSONArray(i);
      networkEdges.add(new Tuple2<>(edge.getInt(0), edge.getInt(1)));
      System.out.println("edge: " + edge.getInt(0) + " - " + edge.getInt(1));
    }
    System.out.println("\n");
    return networkEdges;
  }
}

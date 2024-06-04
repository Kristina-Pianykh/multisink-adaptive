import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import org.json.JSONArray;
import org.json.JSONObject;

public class Playground {

  public static class InputRules {
    String inputEvent;
    boolean forwarded;

    public InputRules(String inputEvent, boolean forwarded) {
      this.inputEvent = inputEvent;
      this.forwarded = forwarded;
    }
  }

  public static class InputRulesForwarded extends InputRules {
    Integer inputOrigin;
    HashMap<Integer, ArrayList<Integer>> forwardingRules;

    public InputRulesForwarded(
        String inputEvent,
        Integer inputOrigin,
        boolean forwarded,
        HashMap<Integer, ArrayList<Integer>> forwardingRules) {
      super(inputEvent, forwarded);
      this.inputOrigin = inputOrigin;
      this.forwardingRules = forwardingRules;
    }
  }

  public static class InputRulesNotForwarded extends InputRules {

    public InputRulesNotForwarded(String inputEvent, boolean forwarded) {
      super(inputEvent, forwarded);
    }
  }

  public static class InevNode {
    String query;
    Integer node;
    boolean isPartInput;

    public InevNode(String query, Integer node, boolean isPartInput) {
      this.query = query;
      this.node = node;
    }

    public String toString() {
      return "(" + query + ", " + node + ")";
    }
  }

  public static class InevEdge {
    InevNode src;
    InevNode dst;

    public InevEdge(InevNode src, InevNode dst) {
      this.src = src;
      this.dst = dst;
    }

    public String toString() {
      return src.toString() + " -> " + dst.toString();
    }
  }

  public static class JsonParser {

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

    public ArrayList<InputRules> parseForwardingRules(JSONObject jsonObject) {
      /* {'F': {'F1': {1: [2], 2: [0], 0: [4]}, 'F3': {3: [4], 4: [0]}}, 'A': {'A': {}}, SEQ(A, F): {'A0F': {0: [4]}}, 'B': {'B': {}}} */
      ArrayList<InputRules> forwardingRules = new ArrayList<>();
      for (String input : jsonObject.keySet()) {
        System.out.println("input event: " + input);
        JSONObject inputRules = jsonObject.getJSONObject(input);
        for (String inputNode : inputRules.keySet()) {
          System.out.println("input node: " + inputNode);
          try {
            Integer srcNode = Integer.parseInt(inputNode.replaceAll("[^0-9]", ""));
            JSONObject hops = inputRules.getJSONObject(inputNode);
            HashMap<Integer, ArrayList<Integer>> srcDstMap = new HashMap<>();
            for (String src : hops.keySet()) {
              Integer srcHop = Integer.parseInt(src);
              System.out.println("src hop: " + srcHop);
              JSONArray dtsHopsJson = hops.getJSONArray(src);
              ArrayList<Integer> dstsHops = new ArrayList<>();
              System.out.println("dst hops: ");
              for (int i = 0; i < dtsHopsJson.length(); i++) {
                dstsHops.add(dtsHopsJson.getInt(i));
                System.out.println(dtsHopsJson.getInt(i));
              }
              srcDstMap.put(srcHop, dstsHops);
              InputRulesForwarded inputRulesForwarded =
                  new InputRulesForwarded(input, srcNode, true, srcDstMap);
              forwardingRules.add(inputRulesForwarded);
            }
          } catch (NumberFormatException e) {
            System.out.println("input not forwarded");
            InputRulesNotForwarded inputRulesNotForwarded =
                new InputRulesNotForwarded(input, false);
            forwardingRules.add(inputRulesNotForwarded);
          }
        }
      }
      return forwardingRules;
    }

    public HashMap<String, ArrayList<String>> parseProjectionInputs(JSONObject jsonObject) {

      /* {'SEQ(A, F)': ['F', 'A'], 'SEQ(A, B, F)': ['SEQ(A, F)', 'B']} */
      HashMap<String, ArrayList<String>> projectionInputs = new HashMap<>();
      for (String projection : jsonObject.keySet()) {
        System.out.println("projection: " + projection);
        JSONArray inputsJson = jsonObject.getJSONArray(projection);
        ArrayList<String> inputs = new ArrayList<>();
        System.out.println("inputs: ");
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
        System.out.println("node: " + nodeStr);
        JSONArray eventTypesJson = jsonObject.getJSONArray(nodeStr);
        ArrayList<String> eventTypes = new ArrayList<>();
        System.out.println("event types: ");
        for (int i = 0; i < eventTypesJson.length(); i++) {
          eventTypes.add(eventTypesJson.getString(i));
          System.out.println(eventTypesJson.getString(i));
        }
        eventAssignments.put(Integer.parseInt(nodeStr), eventTypes);
      }
      return eventAssignments;
    }
  }

  public static String getHighLevelQuery(HashMap<String, ArrayList<String>> projInputs) {
    int maxProjLength = 0;
    String highLevelQuery = null;
    for (String proj : projInputs.keySet()) {
      if (proj.length() > maxProjLength) {
        maxProjLength = proj.length();
        highLevelQuery = proj;
      }
    }
    return highLevelQuery;
  }

  public static ArrayList<InevNode> getInevNodes(
      HashMap<Integer, ArrayList<String>> evalPlan,
      HashMap<Integer, ArrayList<String>> eventAssignments,
      HashMap<String, ArrayList<String>> projInputs,
      String partInput) {

    ArrayList<InevNode> inevNodes = new ArrayList<>();
    InevNode inevNode;
    for (Integer node : evalPlan.keySet()) {
      ArrayList<String> queries = evalPlan.get(node);
      if (queries.size() != 0) {
        for (String query : queries) {
          inevNode = new InevNode(query, node, query.equals(partInput));
          inevNodes.add(inevNode);
          System.out.println("added INEv Node: " + inevNode.query + ", " + inevNode.node);
        }
      }
    }

    for (String query : projInputs.keySet()) {
      ArrayList<String> inputs = projInputs.get(query);
      for (String input : inputs) {

        for (Integer node : eventAssignments.keySet()) {
          ArrayList<String> eventTypes = eventAssignments.get(node);
          if (eventTypes.contains(input)) {
            inevNode = new InevNode(input, node, input.equals(partInput));
            inevNodes.add(inevNode);
            System.out.println("added INEv Node: " + inevNode.query + ", " + inevNode.node);
          }
        }
      }
    }

    return inevNodes;
  }

  public static String getMiltiSinkQuery(HashMap<Integer, ArrayList<String>> evalPlan) {
    String multiSinkQuery = null;
    HashMap<String, Integer> queryCount = new HashMap<>();
    for (Integer node : evalPlan.keySet()) {
      ArrayList<String> queries = evalPlan.get(node);
      if (queries.size() == 0) {
        continue;
      }
      for (String query : queries) {
        queryCount.put(query, queryCount.getOrDefault(query, 0) + 1);
      }
    }

    for (String query : queryCount.keySet()) {
      if (queryCount.get(query) > 1) {
        multiSinkQuery = query;
      }
    }
    return multiSinkQuery;
  }

  public static String getPartInput(
      String multiSinkQuery,
      HashMap<String, ArrayList<String>> projInputs,
      ArrayList<InputRules> forwardingRules) {

    String partInput = null;
    ArrayList<String> inputs = projInputs.get(multiSinkQuery);
    for (String input : inputs) {
      for (InputRules rules : forwardingRules) {
        if (rules.inputEvent.equals(input) && !rules.forwarded) {}
        partInput = input;
      }
    }
    return partInput;
  }

  public static ArrayList<InevEdge> getInevEdges(
      ArrayList<InevNode> inevNodes,
      String partInput,
      HashMap<String, ArrayList<String>> projInputs) {

    ArrayList<InevEdge> edges = new ArrayList<>();
    InevEdge edge;

    for (InevNode dstInevNode : inevNodes) {
      if (!projInputs.keySet().contains(dstInevNode.query)) {
        continue;
      }
      ArrayList<String> inputs = projInputs.get(dstInevNode.query);
      for (String input : inputs) {
        for (InevNode inevNode : inevNodes) {
          if (inevNode.query.equals(input)) {

            if (input.equals(partInput) && inevNode.node != dstInevNode.node) {
              continue;
            }
            edge = new InevEdge(inevNode, dstInevNode);
            System.out.println("added INEv Edge: \n" + edge);
            edges.add(edge);
          }
        }
      }
    }

    return edges;
  }

  public static void main(String[] args) {
    String basePath = "/Users/krispian/Uni/bachelorarbeit/generate_flink_inputs/plans/";
    String evalPlanPath = basePath + "evaluation_plan.json";
    System.out.println(evalPlanPath);
    String forwardingRulesPath = basePath + "forwarding_rules.json";
    System.out.println(forwardingRulesPath);
    String projInputsPath = basePath + "projection_inputs.json";
    String eventAssignmentsPath = basePath + "event_assignment.json";
    JsonParser parser = new JsonParser();
    try {
      JSONObject jsonObject = parser.parseJsonFile(evalPlanPath);
      HashMap<Integer, ArrayList<String>> evalPlan = parser.parseEvaluationPlan(jsonObject);
      JSONObject jsonObject1 = parser.parseJsonFile(forwardingRulesPath);
      ArrayList<InputRules> forwardingRules = parser.parseForwardingRules(jsonObject1);
      JSONObject jsonObject2 = parser.parseJsonFile(projInputsPath);
      HashMap<String, ArrayList<String>> projInputs = parser.parseProjectionInputs(jsonObject2);
      JSONObject jsonObject3 = parser.parseJsonFile(eventAssignmentsPath);
      HashMap<Integer, ArrayList<String>> eventAssignments =
          parser.parseEventAssignments(jsonObject3);

      String multiSinkQuery = getMiltiSinkQuery(evalPlan);
      String partInput = getPartInput(multiSinkQuery, projInputs, forwardingRules);
      System.out.println("multiSinkQuery: " + multiSinkQuery);
      System.out.println("partInput: " + partInput);
      ArrayList<InevNode> inevNodes =
          getInevNodes(evalPlan, eventAssignments, projInputs, partInput);
      ArrayList<InevEdge> inevEdges = getInevEdges(inevNodes, partInput, projInputs);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

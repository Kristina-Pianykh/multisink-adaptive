import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONObject;

public class Playground {

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

          ArrayList<String> inputs = projInputs.get(query);
          inevNode = new InevNode(query, node, inputs, query.equals(partInput), false);
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
            inevNode = new InevNode(input, node, new ArrayList<>(), input.equals(partInput), true);
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
      Set<InputRules> forwardingRules) {

    String partInput = null;
    ArrayList<String> inputs = projInputs.get(multiSinkQuery);
    for (String input : inputs) {
      for (InputRules rules : forwardingRules) {
        if (rules.inputEvent.equals(input) && !rules.forwarded) {
          partInput = input;
        }
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

  // public static Integer getFallbackNode(ArrayList<InevNode> inevNodes) {}

  public static void main(String[] args) {
    // String basePath = "/Users/krispian/Uni/bachelorarbeit/generate_flink_inputs/plans/";
    String basePath =
        "/Users/krispian/Uni/bachelorarbeit/test_flink_inputs/generate_flink_inputs/plans/";
    String evalPlanPath = basePath + "evaluation_plan.json";
    String forwardingRulesPath = basePath + "forwarding_rules.json";
    String projInputsPath = basePath + "projection_inputs.json";
    String eventAssignmentsPath = basePath + "event_assignment.json";
    String networkEdgesPath = basePath + "network_edges.json";

    JsonParser parser = new JsonParser();
    try {
      JSONObject jsonObject = parser.parseJsonFile(evalPlanPath);
      HashMap<Integer, ArrayList<String>> evalPlan = parser.parseEvaluationPlan(jsonObject);
      JSONObject jsonObject1 = parser.parseJsonFile(forwardingRulesPath);
      Set<InputRules> forwardingRules = parser.parseForwardingRules(jsonObject1);
      System.out.println("beginning: size of (forwardingRules): " + forwardingRules.size());
      for (InputRules rule : forwardingRules) {
        System.out.println(rule.toString());
      }

      JSONObject jsonObject2 = parser.parseJsonFile(projInputsPath);
      HashMap<String, ArrayList<String>> projInputs = parser.parseProjectionInputs(jsonObject2);
      JSONObject jsonObject3 = parser.parseJsonFile(eventAssignmentsPath);
      HashMap<Integer, ArrayList<String>> eventAssignments =
          parser.parseEventAssignments(jsonObject3);

      // union on all atomic events generated and query placements
      HashMap<Integer, ArrayList<String>> allPlacements = new HashMap<>();
      for (Integer node : evalPlan.keySet()) {
        ArrayList<String> placements = new ArrayList<>();
        placements.addAll(evalPlan.get(node));
        placements.addAll(eventAssignments.get(node));
        allPlacements.put(node, placements);
      }

      // parse network edges
      JSONObject jsonObjectEdges = parser.parseJsonFile(networkEdgesPath);
      ArrayList<Tuple2<Integer, Integer>> networkEdges = parser.parseNetworkEdges(jsonObjectEdges);

      // create INEv nodes and edges
      String multiSinkQuery = getMiltiSinkQuery(evalPlan);
      String partInput = getPartInput(multiSinkQuery, projInputs, forwardingRules);
      System.out.println("multiSinkQuery: " + multiSinkQuery);
      System.out.println("partInput: " + partInput);
      ArrayList<InevNode> inevNodes =
          getInevNodes(evalPlan, eventAssignments, projInputs, partInput);
      ArrayList<InevEdge> inevEdges = getInevEdges(inevNodes, partInput, projInputs);

      // create list of actual nodes
      // ArrayList<Node> nodes = new ArrayList<>();
      HashMap<Integer, Node> nodes = new HashMap<>();
      for (Integer nodeID : evalPlan.keySet()) {
        String nodeConfigPath = basePath + "config_" + nodeID + ".json";
        System.out.println("node config path: " + nodeConfigPath);
        JSONObject jsonObject4 = parser.parseJsonFile(nodeConfigPath);
        ArrayList<NodeForwardingRules> rules = parser.parseNodeRules(jsonObject4);

        HashMap<String, ArrayList<String>> projInputMap = new HashMap<>();
        if (evalPlan.get(nodeID).size() > 0) {
          for (String query : evalPlan.get(nodeID)) {
            if (projInputs.containsKey(query)) {
              projInputMap.put(query, projInputs.get(query));
            }
          }
        }
        Node nodeObj = new Node(nodeID, eventAssignments.get(nodeID), rules, projInputMap);
        nodes.put(nodeID, nodeObj);
        System.out.println(nodeObj.toString());
      }

      // generate a set of linked lists, each specifying origin of input -> ... -> target
      System.out.println("number of forwarding rules from forwardingDict" + forwardingRules.size());
      for (InputRules rulesPerInput : forwardingRules) {
        System.out.println("forwarded rule being processed now: " + rulesPerInput.toString());
        if (rulesPerInput.forwarded) {
          InputRulesForwarded rules = (InputRulesForwarded) rulesPerInput;
          System.out.println("Linked Lists Paths: Origin -> ... -> Target");
          System.out.println("Input: " + rules.inputEvent);
          Set<LinkedList<Node>> lnkdLists = rules.getAllPathsPerInput(nodes);
          System.out.println("Size of the set of lists: " + lnkdLists.size());

          for (LinkedList<Node> lst : lnkdLists) {
            System.out.println(lst.stream().map(item -> item.nodeID).collect(Collectors.toList()));
          }
          System.out.println("\n\n");
        }
      }

      // Graph<Node, ArrayList<Tuple2<Integer, Integer>>> network =
      //     GraphBuilder.buildGraph(nodes, networkEdges);
      // System.out.println(network.toString());

      // determine the fallback node
      Node fallbackNode = null;
      for (Node node : nodes.values()) {

        if (!node.projProcessed.isEmpty()) {
          System.out.println("processing node: " + node.nodeID);
          boolean isMultiSink = node.projProcessed.containsKey(multiSinkQuery);
          boolean isInputToLocalQuery =
              node.projProcessed.values().stream()
                  .anyMatch(inputs -> inputs.contains(multiSinkQuery));
          boolean isSinkToLocalQuery =
              node.projProcessed.getOrDefault(multiSinkQuery, new ArrayList<>()).stream()
                  .anyMatch(node.projProcessed::containsKey);
          System.out.println("isMultiSink: " + isMultiSink);
          System.out.println("processMultiSinkQueryLocally: " + isInputToLocalQuery);
          System.out.println("sinkToLocalQuery: " + isSinkToLocalQuery);
          if (isMultiSink && (isInputToLocalQuery || isSinkToLocalQuery)) {
            fallbackNode = node;
            break;
          }
        }
      }
      if (fallbackNode == null) {
        int maxForwardRuleCount = 0;
        for (Node node : nodes.values()) {
          boolean isMultiSink = node.projProcessed.containsKey(multiSinkQuery);
          if ((node.forwardingRules.size() > maxForwardRuleCount) && isMultiSink) {
            maxForwardRuleCount = node.forwardingRules.size();
            fallbackNode = node;
          }
        }
      }
      assert fallbackNode != null;
      System.out.println("fallback node: " + fallbackNode.nodeID);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

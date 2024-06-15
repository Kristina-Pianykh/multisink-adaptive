import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

    ArrayList<String> inputs = projInputs.get(multiSinkQuery);
    List<String> delta = new ArrayList<>();
    delta.addAll(inputs);

    ArrayList<String> forwardedRules =
        forwardingRules.stream()
            .filter(rule -> rule.forwarded)
            .map(rule -> (InputRulesForwarded) rule)
            .map(rule -> rule.inputEvent)
            .collect(Collectors.toCollection(ArrayList::new));
    System.out.println("forwardedRules: " + forwardedRules);
    delta.removeAll(forwardedRules);
    System.out.println("delta: " + delta);

    assert delta.size() == 1;
    return delta.get(0);
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

  public static HashMap<InevNode, Set<InevNode>> buildInevGraph(
      ArrayList<InevNode> inevNodes, ArrayList<InevEdge> inevEdges) {
    HashMap<InevNode, Set<InevNode>> neighbours = new HashMap<>();
    for (InevEdge edge : inevEdges) {
      InevNode src = edge.src;
      InevNode dst = edge.dst;
      // neighbours.putIfAbsent(src, new HashSet<>());
      // neighbours.get(src).add(dst);
      neighbours.putIfAbsent(dst, new HashSet<>());
      neighbours.get(dst).add(src);
    }

    for (InevNode node : neighbours.keySet()) {
      System.out.println("Node: ");
      System.out.println("\n  " + node.toString() + "\n");
      for (InevNode neighbor : neighbours.get(node)) {
        System.out.println("  " + neighbor.toString());
      }
    }
    return neighbours;
  }

  public static HashMap<String, Set<LinkedList<Node>>> getCriticalPaths(
      HashMap<InevNode, Set<InevNode>> inevGraph,
      Integer targetNodeID,
      String multiSinkQuery,
      HashMap<Integer, Node> nodes) {
    HashMap<String, Set<LinkedList<Node>>> criticalPaths = new HashMap<>();
    Set<InevNode> neighbours = null;
    for (InevNode inevNode : inevGraph.keySet()) {
      if (inevNode.node == targetNodeID && inevNode.query.equals(multiSinkQuery)) {
        neighbours = inevGraph.get(inevNode);
        System.out.println("identified the InevNode for the fallback node: " + inevNode.toString());
        System.out.println("number of sources: " + neighbours.size());
        break;
      }
    }
    for (InevNode inputSource : neighbours) {
      System.out.println("source query: " + inputSource.query);
      System.out.println("source nodeID: " + inputSource.node);
      Integer nodeId = inputSource.node;
      Set<LinkedList<Node>> networkPath = nodes.get(nodeId).inputTargetPaths.get(inputSource.query);
      if (networkPath == null) {
        continue;
      }

      for (LinkedList<Node> path : networkPath) {
        System.out.println("\n Path: ");
        LinkedList<Node> truncatedPath = new LinkedList<>();
        if (!path.contains(nodes.get(targetNodeID))) {
          continue;
        }
        for (Node node : path) {
          truncatedPath.add(node);
          System.out.println(" -> node: " + node.nodeID);
          if (node.nodeID == targetNodeID) {
            break;
          }
        }
        criticalPaths.putIfAbsent(inputSource.query, new HashSet<>());
        criticalPaths.get(inputSource.query).add(truncatedPath);
        System.out.println("size of critical paths: " + criticalPaths.size());
      }
      // System.out.println(nodes.get(nodeId).inputTargetPaths.toString());
      // System.out.println("networkPath: " + networkPath);
      // criticalPaths.getOrDefault(inputSource.query, new HashSet<>()).addAll(networkPath);
    }
    criticalPaths.forEach(
        (input, paths) -> {
          paths.removeIf(path -> path.size() <= 1);
        });
    // System.out.println("critical Paths: ");
    // for (String input : criticalPaths.keySet()) {
    //   System.out.println("Input: " + input);
    //   for (LinkedList<Node> path : criticalPaths.get(input)) {
    //     System.out.println(
    //         path.stream().map(node -> node.nodeID).collect(Collectors.toList()).toString());
    //   }
    // }
    return criticalPaths;
  }

  public static HashMap<String, Set<LinkedList<Node>>> getInevPairsToRemove(
      HashMap<InevNode, Set<InevNode>> inevGraph,
      String multiSinkQuery,
      HashMap<Integer, Node> nodes,
      ArrayList<Node> nonFallbackNodes) {

    HashMap<String, Set<LinkedList<Node>>> allInvalidPaths = new HashMap<>();
    for (Node nonFallbackNode : nonFallbackNodes) {

      HashMap<String, Set<LinkedList<Node>>> invalidPathsPerInevTargetNode =
          getCriticalPaths(inevGraph, nonFallbackNode.nodeID, multiSinkQuery, nodes);

      // allInvalidPaths.putAll(invalidPathsPerInevTargetNode);
      for (String input : invalidPathsPerInevTargetNode.keySet()) {
        allInvalidPaths.putIfAbsent(input, new HashSet<>());
        allInvalidPaths.get(input).addAll(invalidPathsPerInevTargetNode.get(input));
      }
    }

    System.out.println("\n\nInvalid Paths:");
    for (String input : allInvalidPaths.keySet()) {
      System.out.println("Input: " + input);
      for (LinkedList<Node> path : allInvalidPaths.get(input)) {
        System.out.println(
            path.stream().map(node -> node.nodeID).collect(Collectors.toList()).toString());
      }
    }
    return allInvalidPaths;
  }

  public static HashMap<String, HashMap<Integer, Integer>> nodeRulesToRemove(
      HashMap<String, Set<LinkedList<Node>>> criticalPaths,
      HashMap<String, Set<LinkedList<Node>>> invalidPaths,
      HashMap<Integer, Node> nodes,
      String multiSinkQuery) {

    HashMap<String, HashMap<Integer, Integer>> rulesToRemove = new HashMap<>();
    // HashMap<String, Node> nodesByInput = new HashMap<>();
    HashMap<String, ArrayList<ArrayList<Integer>>> criticalHopsPerInput = new HashMap<>();
    HashMap<String, ArrayList<ArrayList<Integer>>> invalidHopsPerInput = new HashMap<>();

    // for (Node node : nodes.values()) {
    //   for (NodeForwardingRules rule : node.forwardingRules) {
    //     nodesByInput.putIfAbsent(rule.event, new HashMap<>());
    //     nodesByInput.get(rule.event).put(node.nodeID, rule.dstNode);
    //   }
    // }

    for (String input : criticalPaths.keySet()) {
      criticalHopsPerInput.put(input, new ArrayList<>());
      for (LinkedList<Node> path : criticalPaths.get(input)) {
        for (int i = 0; i < path.size() - 1; i++) {
          Node srcNode = path.get(i);
          Node dstNode = path.get(i + 1);
          ArrayList<Integer> hops = new ArrayList<>();
          hops.add(srcNode.nodeID);
          hops.add(dstNode.nodeID);
          criticalHopsPerInput.get(input).add(hops);
        }
      }
    }

    for (String input : invalidPaths.keySet()) {
      invalidHopsPerInput.put(input, new ArrayList<>());
      for (LinkedList<Node> path : invalidPaths.get(input)) {
        for (int i = 0; i < path.size() - 1; i++) {
          Node srcNode = path.get(i);
          Node dstNode = path.get(i + 1);
          ArrayList<Integer> hops = new ArrayList<>();
          hops.add(srcNode.nodeID);
          hops.add(dstNode.nodeID);
          invalidHopsPerInput.get(input).add(hops);
        }
      }
    }

    for (String input : invalidHopsPerInput.keySet()) {
      System.out.println("input: " + input);
      ArrayList<ArrayList<Integer>> hops = invalidHopsPerInput.get(input);
      for (ArrayList<Integer> hop : hops) {
        Integer srcNodeID = hop.get(0);
        Integer dstNodeID = hop.get(1);

        System.out.println("criticalHopsPerInput.get(input): " + criticalHopsPerInput.get(input));
        System.out.println("hop: " + hop);
        if (criticalHopsPerInput.get(input).contains(hop)) {
          System.out.println("hop is critical\n");
          continue;
        }

        System.out.println(
            "removing forwarding rule for "
                + input
                + " from node "
                + srcNodeID
                + " -> "
                + dstNodeID);
        nodes.get(srcNodeID).forwardingRules.remove(new NodeForwardingRules(input, dstNodeID));
      }
    }
    return rulesToRemove;
  }

  public static void main(String[] args) {
    // String basePath = "/Users/krispian/Uni/bachelorarbeit/generate_flink_inputs/plans/";
    String basePath =
        "/Users/krispian/Uni/bachelorarbeit/test_flink_inputs/generate_flink_inputs/plans/";
    String evalPlanPath = basePath + "evaluation_plan.json";
    String forwardingRulesPath = basePath + "forwarding_rules.json";
    String projInputsPath = basePath + "projection_inputs.json";
    String eventAssignmentsPath = basePath + "event_assignment.json";
    String networkEdgesPath = basePath + "network_edges.json";
    String steinerTreeSizePath = basePath + "steiner_tree_size.json";

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
      HashMap<InevNode, Set<InevNode>> inevGraph = buildInevGraph(inevNodes, inevEdges);

      // create list of actual nodes
      // ArrayList<Node> nodes = new ArrayList<>();
      HashMap<Integer, Node> nodes = new HashMap<>();
      for (Integer nodeID : evalPlan.keySet()) {
        String nodeConfigPath = basePath + "config_" + nodeID + ".json";
        System.out.println("node config path: " + nodeConfigPath);
        JSONObject jsonObject4 = parser.parseJsonFile(nodeConfigPath);
        Set<NodeForwardingRules> rules = parser.parseNodeRules(jsonObject4);

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
          Set<LinkedList<Node>> inputOriginTargetSet = rules.getAllPathsPerInput(nodes);

          for (LinkedList<Node> lst : inputOriginTargetSet) {
            Node headNode = lst.getFirst();
            headNode.inputTargetPaths.put(rules.inputEvent, inputOriginTargetSet);
            System.out.println(lst.stream().map(item -> item.nodeID).collect(Collectors.toList()));
          }
          System.out.println("\n\n");
        }
      }

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
        System.out.println("no fallback node found. Using node with most forwarding rules.");
        int maxForwardRuleCount = 0;
        for (Node node : nodes.values()) {
          boolean isMultiSink = node.projProcessed.containsKey(multiSinkQuery);
          System.out.println("processing node: " + node.nodeID);
          System.out.println("number of forwarding rules: " + node.forwardingRules.size());
          if ((node.forwardingRules.size() > maxForwardRuleCount) && isMultiSink) {
            maxForwardRuleCount = node.forwardingRules.size();
            fallbackNode = node;
          }
        }
      }
      assert fallbackNode != null;
      nodes.get(fallbackNode.nodeID).fallbackNode = true;
      System.out.println("fallback node: " + fallbackNode.nodeID);

      // determine critical paths for the inputs of Q to the fallback node
      HashMap<String, Set<LinkedList<Node>>> criticalPaths =
          getCriticalPaths(inevGraph, fallbackNode.nodeID, multiSinkQuery, nodes);
      // System.out.println("\nCritical Paths:");
      // System.out.println("number of critical paths: " + criticalPaths.size());
      // for (String input : criticalPaths.keySet()) {
      //   System.out.println("Input: " + input);
      //   for (LinkedList<Node> path : criticalPaths.get(input)) {
      //     System.out.println(
      //         path.stream().map(node -> node.nodeID).collect(Collectors.toList()).toString());
      //   }
      // }

      // adaptive strategy
      // step 1. Remove processing of multiSinkQuery from all non-fallback nodes
      ArrayList<Node> nonFallbackNodes = new ArrayList<>();
      for (Node node : nodes.values()) {
        if (!node.fallbackNode && node.projProcessed.containsKey(multiSinkQuery)) {
          nonFallbackNodes.add(node);
        }
      }
      System.out.println(
          "nonFallbackNodes: " + nonFallbackNodes.stream().map(n -> n.nodeID).toList());

      for (Node node : nodes.values()) {
        boolean isMultiSinkNonFallbackNode =
            (!node.fallbackNode) && node.projProcessed.containsKey(multiSinkQuery);
        if (isMultiSinkNonFallbackNode) {
          System.out.println("removing query Q and forrwarding of Q from node: " + node.nodeID);
          node.projProcessed.remove(multiSinkQuery); // remove query Q
          node.inputTargetPaths.remove(multiSinkQuery); // remove forwarding of Q matches
          System.out.println("processing queries after removal: " + node.projProcessed.keySet());
          System.out.println(
              "forwarding rules after removal now only for the following queries: "
                  + node.inputTargetPaths.keySet());
        }
      }
      System.out.println("\n\n");

      // step 2. Remove the rules that forward inputs of Q to non-fallback nodes
      HashMap<String, Set<LinkedList<Node>>> invalidPaths =
          getInevPairsToRemove(inevGraph, multiSinkQuery, nodes, nonFallbackNodes);
      ArrayList<String> multiSinkQueryInputs = projInputs.get(multiSinkQuery);

      System.out.println("REMOVING NEW ALGORUTHM");
      nodeRulesToRemove(criticalPaths, invalidPaths, nodes, multiSinkQuery);

      // for (Node node : nodes.values()) {
      //   for (String multiSinkQueryInput : multiSinkQueryInputs) {
      //     Set<LinkedList<Node>> inputPaths = node.inputTargetPaths.get(multiSinkQueryInput);
      //     if (inputPaths != null) {
      //       Set<LinkedList<Node>> updatedInputPaths = new HashSet<>();
      //       for (LinkedList<Node> inputPath : inputPaths) {
      //         Node targetNode = inputPath.getLast();
      //         if (!nonFallbackNodes.contains(targetNode)) {
      //           updatedInputPaths.add(inputPath);
      //         } else {
      //           System.out.println(
      //               "removing forwarding rule from ORIGIN "
      //                   + node.nodeID
      //                   + " -> ... -> TARGET "
      //                   + targetNode.nodeID
      //                   + " for input: "
      //                   + multiSinkQueryInput);
      //           // traverse the linked list and remove the NodeForwardingRules
      //           // from each node on the way for the given input
      //           for (int i = 0; i < inputPath.size() - 1; i++) {
      //             Node srcNode = inputPath.get(i);
      //             Node destNode = inputPath.get(i + 1);
      //             NodeForwardingRules srcDstPair =
      //                 new NodeForwardingRules(multiSinkQueryInput, destNode.nodeID);
      //             srcNode.forwardingRules.remove(srcDstPair);
      //             System.out.println(
      //                 "Removed forwarding rule for node "
      //                     + srcNode.nodeID
      //                     + " "
      //                     + i
      //                     + " -> "
      //                     + destNode.nodeID);
      //             // one hop before
      //             if (i > 0) {
      //               srcDstPair = new NodeForwardingRules(multiSinkQueryInput, destNode.nodeID);
      //               srcNode.forwardingRules.remove(srcDstPair);
      //               System.out.println(
      //                   "Removed forwarding rule for node "
      //                       + srcNode.nodeID
      //                       + " "
      //                       + (i - 1)
      //                       + " -> "
      //                       + destNode.nodeID);
      //             }
      //           }
      //         }
      //       }
      //       node.inputTargetPaths.put(multiSinkQueryInput, updatedInputPaths);
      //       System.out.println("updated node rules: " + node.forwardingRules.toString());
      //       System.out.println("\n\n\n");
      //     }
      //   }
      // }

      // step 3. Compute shortest path from non-fallback nodes to fallback node
      HashMap<Integer, Set<Integer>> nodeNeighbors = Graph.getAllNeighbors(networkEdges);
      for (Integer node : nodeNeighbors.keySet()) {
        System.out.println("Node: " + node + " Neighbors: " + nodeNeighbors.get(node).toString());
      }
      for (Node node : nonFallbackNodes) {
        System.out.println("Node: " + node.nodeID);
        Set<LinkedList<Integer>> allPaths =
            Graph.findAllPaths(node.nodeID, fallbackNode.nodeID, nodeNeighbors);
        LinkedList<Integer> shortestPath = Graph.getShortestPath(allPaths);
        System.out.println(
            "Shortest Path from "
                + node.nodeID
                + " to "
                + fallbackNode.nodeID
                + ":"
                + shortestPath);

        System.out.println("\n\nSHORTEST PATH SELECTED: " + shortestPath.toString() + "\n\n\n");
        // add the new path for each input to Q generated by a non-fallback node
        // to forward to the fallback node
        List<String> relevantQInputs =
            Stream.concat(node.eventsGenerated.stream(), node.projProcessed.keySet().stream())
                .filter(input -> multiSinkQueryInputs.contains(input))
                .collect(Collectors.toList());
        for (String input : relevantQInputs) {
          Set<LinkedList<Node>> newPath = new HashSet<>();
          newPath.add(
              shortestPath.stream()
                  .map(nodes::get)
                  .collect(Collectors.toCollection(LinkedList::new)));
          node.inputTargetPaths.put(input, newPath);
        }

        // add forwarding rule fo reach node on the path
        for (int i = 0; i < shortestPath.size() - 1; i++) {
          Node srcNode = nodes.get(shortestPath.get(i));
          Node destNode = nodes.get(shortestPath.get(i + 1));
          for (String input : relevantQInputs) {
            srcNode.forwardingRules.add(new NodeForwardingRules(input, destNode.nodeID));
            System.out.println(
                "added forwarding rule: "
                    + srcNode.nodeID
                    + " -> "
                    + destNode.nodeID
                    + " for input: "
                    + input);
          }
        }
      }

      for (Node node : nodes.values()) {
        System.out.println("Node: " + node.nodeID);
        System.out.println("local forwarding rules:");
        for (NodeForwardingRules rule : node.forwardingRules) {
          System.out.println(rule.toString());
        }
        System.out.println("Processing queries: " + node.projProcessed.keySet());
        System.out.println("\n\n\n");
      }

      // save the info required for the calculation of the inequality
      // to monitor for rates to change
      InequalityInputs inequalityInputs = new InequalityInputs();
      inequalityInputs.multiSinkQuery = multiSinkQuery;
      inequalityInputs.numMultiSinkNodes = nonFallbackNodes.size() + 1;
      inequalityInputs.multiSinkNodes =
          nonFallbackNodes.stream()
              .map(n -> n.nodeID)
              .collect(Collectors.toCollection(ArrayList::new));
      inequalityInputs.multiSinkNodes.add(fallbackNode.nodeID);
      inequalityInputs.partitioningInput = partInput;
      inequalityInputs.queryInputs = multiSinkQueryInputs;
      inequalityInputs.nonPartitioningInputs =
          multiSinkQueryInputs.stream()
              .filter(input -> !input.equals(partInput))
              .collect(Collectors.toCollection(ArrayList::new));
      JSONObject jsonObjectSt = parser.parseJsonFile(steinerTreeSizePath);
      inequalityInputs.steinerTreeSize = parser.parseSteinerTreeSize(jsonObjectSt);
      inequalityInputs.saveToFile(basePath + "inequality_inputs.json");

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

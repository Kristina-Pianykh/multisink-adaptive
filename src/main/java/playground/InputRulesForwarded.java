import java.lang.Object.*;
import java.util.*;

public class InputRulesForwarded extends InputRules {
  String inputEvent;
  Integer inputOrigin;
  HashMap<Integer, ArrayList<Integer>> forwardingRules;

  public InputRulesForwarded(
      String inputEvent,
      Integer inputOrigin,
      boolean forwarded,
      HashMap<Integer, ArrayList<Integer>> forwardingRules) {
    super(inputEvent, forwarded);
    this.inputEvent = inputEvent;
    this.inputOrigin = inputOrigin;
    this.forwardingRules = forwardingRules;
  }

  public void findPaths(
      Node start,
      LinkedList<Node> path,
      HashMap<Integer, Node> nodes,
      Set<LinkedList<Node>> paths) {
    // Add the current node to the path
    path.add(start);

    // If the start node has no outgoing edges, this is an end of a path
    if (!this.forwardingRules.containsKey(start.nodeID)
        || this.forwardingRules.get(start.nodeID).isEmpty()) {
      // Add the path to the set of paths
      paths.add(new LinkedList<>(path));
    } else {
      // Otherwise, continue the search for each adjacent node
      for (int nextID : this.forwardingRules.get(start.nodeID)) {
        Node next = nodes.get(nextID);
        findPaths(next, path, nodes, paths);
      }
    }

    // Remove the current node from the path to backtrack
    path.removeLast();
  }

  public Set<LinkedList<Node>> getAllPathsPerInput(HashMap<Integer, Node> nodes) {
    Set<LinkedList<Node>> paths = new HashSet<>();
    Set<LinkedList<Node>> pathsComplete = new HashSet<>();

    // Start the search from each node in the graph
    Node headNode = nodes.get(this.inputOrigin);
    findPaths(headNode, new LinkedList<>(), nodes, paths);

    // construct new origin-target paths if some elements in the linked
    // list use the inputEvent for processing queries
    // System.out.println("iterating over paths to find new targets");
    for (LinkedList<Node> path : paths) {
      pathsComplete.add(path);
      // System.out.println("Path: " + path.stream().map(n -> n.nodeID).toList());
      // check if any nodes between the head and tail use the inputEvent
      // for processing queries
      ArrayList<Node> newTargets = new ArrayList<>();
      for (int i = 1; i < (path.size() - 1); i++) {
        Node node = path.get(i);
        // System.out.println("Hop: " + node.nodeID + " " + node.projProcessed);
        // System.out.println(
        //     "inputs of all queries: "
        //         + node.projProcessed.values().stream()
        //             .flatMap(List::stream)
        //             .anyMatch(this.inputEvent::equals));
        if (node.projProcessed.values().stream()
            .flatMap(List::stream)
            .anyMatch(this.inputEvent::equals)) {
          // System.out.println("Found a new target: " + node.nodeID);
          newTargets.add(node);
        }
      }

      // System.out.println("New targets: " + newTargets.stream().map(n -> n.nodeID).toList());
      if (newTargets.size() > 0) {
        for (Node item : newTargets) {
          LinkedList<Node> newOriginTargetPath = new LinkedList<>();
          Iterator<Node> iter = path.iterator();
          while (iter.hasNext()) {
            Node element = iter.next();
            newOriginTargetPath.add(element);
            if (element.nodeID == item.nodeID) {
              break;
            }
          }

          pathsComplete.add(newOriginTargetPath);
        }
      }
    }

    return pathsComplete;
  }

  public String toString() {
    String str = "";
    str += "Input: " + this.inputEvent;
    str += " Source Node: " + this.inputOrigin;
    str += " Forwarding rules: ";
    for (int nodeID : this.forwardingRules.keySet()) {
      str += nodeID + " -> ";
      str += this.forwardingRules.get(nodeID);
    }
    return str;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InputRulesForwarded that = (InputRulesForwarded) o;
    return this.inputEvent.equals(that.inputEvent)
        && this.inputOrigin.equals(that.inputOrigin)
        && this.forwardingRules.equals(that.forwardingRules);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.inputEvent, this.inputOrigin, this.forwardingRules);
  }
}

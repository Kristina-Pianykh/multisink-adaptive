import java.util.*;

public class Node {
  Integer nodeID;
  ArrayList<String> eventsGenerated;
  Set<NodeForwardingRules> forwardingRules;
  HashMap<String, ArrayList<String>> projProcessed;
  HashMap<String, Set<LinkedList<Node>>> inputTargetPaths;
  boolean fallbackNode;

  public Node(
      Integer nodeID,
      ArrayList<String> eventsGenerated,
      Set<NodeForwardingRules> forwardingRules,
      HashMap<String, ArrayList<String>> projProcessed) {
    this.nodeID = nodeID;
    this.eventsGenerated = eventsGenerated;
    this.forwardingRules = forwardingRules;
    this.projProcessed = projProcessed;
    this.inputTargetPaths = new HashMap<>();
    this.fallbackNode = false;
  }

  public String toString() {
    String str = "";
    str += "Node ID: " + nodeID;
    str += "  Events: " + eventsGenerated.toString() + "\n";
    str += "  Forwarding Rules: \n";
    for (NodeForwardingRules rule : forwardingRules) {
      str += "    " + rule.toString();
    }
    str += "  Processing: \n";
    for (String query : projProcessed.keySet()) {
      str += "    " + query + ". Inputs: " + projProcessed.get(query).toString() + "\n";
    }
    str += "  Fallback node: " + this.fallbackNode;
    return str;
  }
}

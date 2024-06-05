import java.util.ArrayList;

public class NodeForwardingRules {
  String event;
  ArrayList<Integer> srcNodes;
  ArrayList<Integer> dstNodes;

  public NodeForwardingRules(
      String event, ArrayList<Integer> srcNodes, ArrayList<Integer> dstNodes) {
    this.event = event;
    this.srcNodes = srcNodes;
    this.dstNodes = dstNodes;
  }

  public String toString() {
    String str = "";
    str += "Event: " + event + "\n";
    str += "Source Nodes: " + srcNodes.toString() + "\n";
    str += "Destination Nodes: " + dstNodes.toString() + "\n";
    return str;
  }
}

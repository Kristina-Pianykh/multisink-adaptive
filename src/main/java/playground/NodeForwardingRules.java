import java.util.*;

public class NodeForwardingRules {
  String event;
  Integer dstNode;

  // Set<Integer> srcNodes;
  // Set<Integer> dstNodes;

  // public NodeForwardingRules(String event, Set<Integer> srcNodes, Set<Integer> dstNodes) {
  //   this.event = event;
  //   this.srcNodes = srcNodes;
  //   this.dstNodes = dstNodes;
  // }

  public NodeForwardingRules(String event, Integer dstNode) {
    this.event = event;
    this.dstNode = dstNode;
    // this.srcNodes = Set.of(srcNodes);
    // this.dstNodes = Set.of(dstNodes);
  }

  public String toString() {
    String str = "";
    str += "Event: " + event + "\n";
    str += "Destination Node: " + dstNode.toString() + "\n";
    return str;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof NodeForwardingRules)) {
      return false;
    }
    NodeForwardingRules rule = (NodeForwardingRules) obj;
    return rule.event.equals(event) && rule.dstNode == dstNode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(event, dstNode);
  }
}

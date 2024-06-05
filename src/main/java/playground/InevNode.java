import java.util.ArrayList;

public class InevNode {
  String query;
  Integer node;
  ArrayList<String> inputs;
  boolean isPartInput;
  boolean atomic;

  public InevNode(
      String query, Integer node, ArrayList<String> inputs, boolean isPartInput, boolean atomic) {
    this.query = query;
    this.node = node;
    this.inputs = inputs;
    this.isPartInput = isPartInput;
    this.atomic = atomic;
  }

  public String toString() {
    return "("
        + query
        + ", "
        + node
        + ")"
        + (this.atomic ? "Inputs: []" : "; Inputs: " + inputs.toString());
  }
}

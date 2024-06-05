public class InevEdge {
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

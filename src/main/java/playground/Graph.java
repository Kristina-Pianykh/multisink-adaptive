import java.util.*;
import org.apache.flink.api.java.tuple.Tuple2;

public class Graph {

  public static class Edge {
    Integer node1;
    Integer node2;

    public Edge(Integer node1, Integer node2) {
      this.node1 = node1;
      this.node2 = node2;
    }
  }

  public static HashMap<Integer, Set<Integer>> getAllNeighbors(
      ArrayList<Tuple2<Integer, Integer>> edges) {
    HashMap<Integer, Set<Integer>> neighbors = new HashMap<>();
    for (Tuple2<Integer, Integer> edge : edges) {
      neighbors.computeIfAbsent(edge.f0, k -> new HashSet<>()).add(edge.f1);
      neighbors.computeIfAbsent(edge.f1, k -> new HashSet<>()).add(edge.f0);
    }
    return neighbors;
  }

  public static boolean stuck(
      Integer src, Integer dest, Set<Integer> visited, HashMap<Integer, Set<Integer>> neighbors) {
    if (src.equals(dest)) {
      return false;
    }
    for (Integer neighbor : neighbors.getOrDefault(src, new HashSet<>())) {
      if (!visited.contains(neighbor)) {
        visited.add(neighbor);
        if (!stuck(neighbor, dest, visited, neighbors)) {
          return false;
        }
        visited.remove(neighbor); // Backtrack
      }
    }
    return true;
  }

  public static void search(
      Integer src,
      Integer dest,
      Stack<Integer> path,
      Set<Integer> visited,
      HashMap<Integer, Set<Integer>> neighbors,
      Set<LinkedList<Integer>> paths) {
    if (src.equals(dest)) {
      paths.add(new LinkedList<>(path));
      // System.out.println(path.toString());
      return;
    }

    visited.add(src);

    for (Integer neighbor : neighbors.getOrDefault(src, new HashSet<>())) {
      if (!visited.contains(neighbor)) {
        path.push(neighbor);
        search(neighbor, dest, path, visited, neighbors, paths);
        path.pop();
      }
    }

    visited.remove(src); // Unmark the current node to allow other paths
  }

  public static Set<LinkedList<Integer>> findAllPaths(
      Integer src, Integer dest, HashMap<Integer, Set<Integer>> neighbors) {
    Stack<Integer> path = new Stack<>();
    Set<Integer> visited = new HashSet<>();
    Set<LinkedList<Integer>> paths = new HashSet<>();
    path.push(src);
    visited.add(src);
    search(src, dest, path, visited, neighbors, paths);
    return paths;
  }

  public static LinkedList<Integer> getShortestPath(
      Set<NodeForwardingRules> forwardingRules, Set<LinkedList<Integer>> paths) {
    List<LinkedList<Integer>> allShortestPaths = new ArrayList<>(paths);
    LinkedList<Integer> shortestPath = new LinkedList<>();
    Integer minPathLength = Integer.MAX_VALUE;
    // System.out.println("Initial minPathLength: " + minPathLength);
    for (LinkedList<Integer> path : paths) {
      // System.out.println("Path: " + path.toString());
      // System.out.println("Size: " + path.size());
      // System.out.println("path.size() <= minPathLength: " + (path.size() <= minPathLength));
      if (path.size() <= minPathLength) {
        allShortestPaths.add(path);
        shortestPath = path;
        minPathLength = path.size();
      }
      for (LinkedList<Integer> p : allShortestPaths) {
        for (NodeForwardingRules rule : forwardingRules) {
          Integer neighbor = p.get(1);
          if (rule.dstNode.equals(neighbor)) return p;
        }
      }
    }
    return shortestPath;
  }

  // public static void main(String[] args) {
  //   ArrayList<Edge> edges = new ArrayList<>();
  //   edges.add(new Edge(0, 2));
  //   edges.add(new Edge(0, 4));
  //   edges.add(new Edge(4, 3));
  //   edges.add(new Edge(3, 4));
  //   edges.add(new Edge(2, 1));
  //   edges.add(new Edge(2, 3)); // Added this edge to make sure path [1, 2, 3, 4] is possible
  //
  //   HashMap<Integer, Set<Integer>> neighbors = getAllNeighbors(edges);
  //   for (Integer node : neighbors.keySet()) {
  //     System.out.println("Node: " + node + " Neighbors: " + neighbors.get(node).toString());
  //   }
  //   findAllPaths(1, 4, neighbors);
  // }
}

package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.util.*;


public class DagPaths {


    private static final String GRAPH_FILE_NAME = "small3.json";
    private static final String SOURCE_NODE = "V1";

    // Global Instrumentation Counters
    private static long relaxationCount = 0;



    static class GraphData {
        public List<String> nodes;
        public List<List<Object>> edges;
        public GraphData() {}
    }


    static class Edge {
        final String destination;
        final int weight;
        public Edge(String destination, int weight) {
            this.destination = destination;
            this.weight = weight;
        }
    }

    // Graph Loading

    private static Map<String, List<Edge>> loadWeightedGraph(Set<String> allNodes) throws Exception {
        InputStream is = DagPaths.class.getClassLoader().getResourceAsStream(GRAPH_FILE_NAME);
        if (is == null) throw new RuntimeException("Error: File not found: " + GRAPH_FILE_NAME);

        GraphData data = new ObjectMapper().readValue(new InputStreamReader(is), GraphData.class);
        Map<String, List<Edge>> G = new HashMap<>();

        for (String node : data.nodes) {
            G.put(node, new ArrayList<>());
            allNodes.add(node);
        }

        for (List<Object> edge : data.edges) {
            if (edge.size() == 3) {
                String u = (String) edge.get(0);
                String v = (String) edge.get(1);
                int w = (Integer) edge.get(2);
                G.computeIfAbsent(u, k -> new ArrayList<>()).add(new Edge(v, w));
                allNodes.add(u);
                allNodes.add(v);
            }
        }
        return G;
    }

    // --- Topological Sort (Prerequisite for Paths) ---

    private static void dfsTopologicalSort(String u, Map<String, List<Edge>> G, Set<String> visited, Stack<String> stack) {
        visited.add(u);
        for (Edge edge : G.getOrDefault(u, Collections.emptyList())) {
            if (!visited.contains(edge.destination)) dfsTopologicalSort(edge.destination, G, visited, stack);
        }
        stack.push(u);
    }

    private static List<String> getTopologicalOrder(Map<String, List<Edge>> G, Set<String> allNodes) {
        Stack<String> stack = new Stack<>();
        Set<String> visited = new HashSet<>();
        for (String node : allNodes) if (!visited.contains(node)) dfsTopologicalSort(node, G, visited, stack);

        List<String> sortedOrder = new ArrayList<>();
        while (!stack.isEmpty()) sortedOrder.add(stack.pop());
        return sortedOrder;
    }

    //  Path Calculation


    private static Map<String, Double> pathInDAG(
            Map<String, List<Edge>> G, List<String> topoOrder, String source,
            Map<String, String> predecessorMap, boolean findShortest) {

        Map<String, Double> dist = new HashMap<>();
        double initialDist = findShortest ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;

        for (String node : topoOrder) {
            dist.put(node, initialDist);
            predecessorMap.put(node, null);
        }
        dist.put(source, 0.0);

        for (String u : topoOrder) {
            double currentDist = dist.get(u);
            if (currentDist != initialDist) {
                for (Edge edge : G.getOrDefault(u, Collections.emptyList())) {
                    relaxationCount++; // Instrumentation: Relaxation counter
                    String v = edge.destination;
                    double newDist = currentDist + edge.weight;

                    // Relaxation step: Min for SSSP, Max for LPSP
                    if (findShortest ? (newDist < dist.get(v)) : (newDist > dist.get(v))) {
                        dist.put(v, newDist);
                        predecessorMap.put(v, u);
                    }
                }
            }
        }
        return dist;
    }

    private static List<String> reconstructPath(String target, String source, Map<String, String> predecessorMap) {
        LinkedList<String> path = new LinkedList<>();
        String current = target;
        while (current != null && !current.equals(source)) {
            path.addFirst(current);
            current = predecessorMap.get(current);
        }
        if (current != null && current.equals(source)) path.addFirst(source);
        return path;
    }


    // Main

    public static void main(String[] args) {
        long startTime = System.nanoTime();

        System.out.println("--- Task 1.3: Shortest and Longest Paths in a DAG ---");
        System.out.printf("Source Node: %s\n", SOURCE_NODE);

        try {
            Set<String> allNodes = new HashSet<>();
            Map<String, List<Edge>> G_weighted = loadWeightedGraph(allNodes);
            List<String> topoOrder = getTopologicalOrder(G_weighted, allNodes);

            //   Shortest Path
            Map<String, String> shortestPredecessorMap = new HashMap<>();
            // reset counter to
            relaxationCount = 0;
            Map<String, Double> shortestDistances = pathInDAG(
                    G_weighted, topoOrder, SOURCE_NODE, shortestPredecessorMap, true);
            long relaxationsSSSP = relaxationCount;

            //  Longest Path
            Map<String, String> longestPredecessorMap = new HashMap<>();
            // counter to 0 before LPSP run
            relaxationCount = 0;
            Map<String, Double> longestDistances = pathInDAG(
                    G_weighted, topoOrder, SOURCE_NODE, longestPredecessorMap, false);
            long relaxationsLPSP = relaxationCount;


            System.out.println("\n--- Single-Source Shortest Paths from " + SOURCE_NODE + " ---");


            String shortestPathTarget = allNodes.contains("V6") ? "V6" : topoOrder.get(topoOrder.size() - 1);

            for (String node : topoOrder) {
                Double dist = shortestDistances.get(node);
                if (dist != Double.POSITIVE_INFINITY) {
                    System.out.printf("  To %s: %.2f\n", node, dist);
                } else {
                    System.out.printf("  To %s: Unreachable\n", node);
                }
            }

            List<String> shortestPath = reconstructPath(shortestPathTarget, SOURCE_NODE, shortestPredecessorMap);
            if (!shortestPath.isEmpty()) {
                System.out.printf("\n  Optimal Shortest Path to %s (Length %.2f):\n", shortestPathTarget, shortestDistances.get(shortestPathTarget));
                System.out.printf("  Path: %s\n", String.join(" -> ", shortestPath));
            }


            // --- Output Longest Path (Critical Path) Results ---
            String criticalPathEndNode = null;
            double maxDist = Double.NEGATIVE_INFINITY;
            for (Map.Entry<String, Double> entry : longestDistances.entrySet()) {
                if (entry.getValue() > maxDist) {
                    maxDist = entry.getValue();
                    criticalPathEndNode = entry.getKey();
                }
            }

            System.out.println("\n--- Longest Path (Critical Path) from " + SOURCE_NODE + " ---");
            if (criticalPathEndNode != null && maxDist != Double.NEGATIVE_INFINITY) {
                List<String> criticalPath = reconstructPath(criticalPathEndNode, SOURCE_NODE, longestPredecessorMap);

                System.out.printf("  Critical Path Length: %.2f\n", maxDist);
                System.out.printf("  Path: %s\n", String.join(" -> ", criticalPath));
            } else {
                System.out.println("  No paths found from source " + SOURCE_NODE);
            }

            long endTime = System.nanoTime();
            double durationMillis = (endTime - startTime) / 1_000_000.0;

            // --- Instrumentation Report ---
            System.out.println("\n--- Instrumentation Report ---");
            System.out.printf("Total Relaxations (SSSP Run): %d\n", relaxationsSSSP);
            System.out.printf("Total Relaxations (LPSP Run): %d\n", relaxationsLPSP);
            System.out.printf("Total Execution Time: %.3f milliseconds\n", durationMillis);
            System.out.println("------------------------------");

        } catch (RuntimeException e) {
            System.err.println("\n❌ A critical error occurred: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("\n❌ An unexpected error occurred during execution.");
            e.printStackTrace();
        }
    }
}

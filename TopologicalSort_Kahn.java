package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Task 1.2: Topological Sort using Kahn's Algorithm on the Condensation DAG.
 * Integrates graph loading, SCC finding, DAG construction, and instrumented Topological Sort.
 */
public class TopologicalSort_Kahn {

    // Change the JSON file name HERE HERE HERE HERE
    private static final String GRAPH_FILE_NAME = "small1.json"; // Assumes file is in src/main/resources
    // HERE

    // Global Instrumentation Counters
    private static long kahnPops = 0;
    private static long kahnPushes = 0;

    // Internal class to map the JSON structure
    static class GraphData {
        public List<String> nodes;
        public List<List<String>> edges;
        public GraphData() {}
    }

    // --- Graph Loading and SCC Preparation (Reused) ---

    private static Map<String, List<String>> loadGraph(String jsonFileName, Set<String> allNodes, GraphData[] dataHolder) throws Exception {
        InputStream is = TopologicalSort_Kahn.class.getClassLoader().getResourceAsStream(jsonFileName);
        if (is == null) throw new RuntimeException("Error: File not found in resources: " + jsonFileName);

        ObjectMapper mapper = new ObjectMapper();
        GraphData data = mapper.readValue(new InputStreamReader(is), GraphData.class);
        dataHolder[0] = data;

        Map<String, List<String>> G = new HashMap<>();
        for (String node : data.nodes) {
            G.put(node, new ArrayList<>());
            allNodes.add(node);
        }
        for (List<String> edge : data.edges) {
            if (edge.size() == 2) {
                String u = edge.get(0);
                String v = edge.get(1);
                G.computeIfAbsent(u, k -> new ArrayList<>()).add(v);
                allNodes.add(u);
                allNodes.add(v);
            }
        }
        return G;
    }

    private static Map<String, List<String>> getTransposeGraph(Map<String, List<String>> G, Set<String> allNodes) {
        Map<String, List<String>> GT = new HashMap<>();
        for (String node : allNodes) GT.put(node, new ArrayList<>());
        for (Map.Entry<String, List<String>> entry : G.entrySet()) {
            for (String v : entry.getValue()) {
                GT.get(v).add(entry.getKey());
            }
        }
        return GT;
    }

    // SCC DFS Pass 1 (Ordering) - Populates stack
    private static void dfsPass1(String u, Map<String, List<String>> G, Set<String> visited, Stack<String> stack) {
        visited.add(u);
        for (String v : G.getOrDefault(u, Collections.emptyList())) {
            if (!visited.contains(v)) dfsPass1(v, G, visited, stack);
        }
        stack.push(u);
    }

    // SCC DFS Pass 2 (Grouping) - Finds one SCC
    private static void dfsPass2(String u, Map<String, List<String>> GT, Set<String> visited, List<String> currentScc) {
        visited.add(u);
        currentScc.add(u);
        for (String v : GT.getOrDefault(u, Collections.emptyList())) {
            if (!visited.contains(v)) dfsPass2(v, GT, visited, currentScc);
        }
    }

    // Executes Kosaraju's Algorithm
    private static List<List<String>> findSCCs(Map<String, List<String>> G, Set<String> allNodes) {
        Stack<String> orderStack = new Stack<>();
        Set<String> visited = new HashSet<>();
        for (String node : allNodes) {
            if (!visited.contains(node)) dfsPass1(node, G, visited, orderStack);
        }

        Map<String, List<String>> GT = getTransposeGraph(G, allNodes);
        List<List<String>> sccs = new ArrayList<>();
        visited.clear();

        while (!orderStack.isEmpty()) {
            String u = orderStack.pop();
            if (!visited.contains(u)) {
                List<String> component = new ArrayList<>();
                dfsPass2(u, GT, visited, component);
                sccs.add(component);
            }
        }
        return sccs;
    }

    // Builds the Condensation DAG and returns the adjacency map and the SCC ID map
    private static Map<List<String>, Set<List<String>>> buildCondensationGraph(
            Map<String, List<String>> G, List<List<String>> sccs, Map<List<String>, String> sccIdMap) {

        Map<List<String>, Set<List<String>>> condAdj = new HashMap<>();
        Map<String, List<String>> nodeToSccMap = new HashMap<>();

        // 1. Map nodes and initialize DAG
        for (int i = 0; i < sccs.size(); i++) {
            List<String> scc = sccs.get(i);
            String sccId = "SCC " + (i + 1);
            sccIdMap.put(scc, sccId);
            condAdj.put(scc, new HashSet<>());
            for (String node : scc) {
                nodeToSccMap.put(node, scc);
            }
        }

        // 2. Find inter-SCC edges
        for (List<String> sccU : sccs) {
            Set<List<String>> neighbors = condAdj.get(sccU);
            for (String u : sccU) {
                for (String v : G.getOrDefault(u, Collections.emptyList())) {
                    List<String> sccV = nodeToSccMap.get(v);
                    if (sccU != sccV) {
                        neighbors.add(sccV);
                    }
                }
            }
        }
        return condAdj;
    }

    // --- Kahn's Topological Sort Implementation ---

    /**
     * Computes the Topological Order of the Condensation DAG using Kahn's algorithm.
     * Tracks queue pushes/pops as instrumentation.
     */
    public static List<List<String>> topologicalSortKahn(
            Map<List<String>, Set<List<String>>> condAdj, List<List<String>> allSccs) {

        // 1. Calculate In-degrees
        Map<List<String>, Integer> inDegree = new HashMap<>();
        for (List<String> scc : allSccs) inDegree.put(scc, 0);

        for (Set<List<String>> neighbors : condAdj.values()) {
            for (List<String> neighborScc : neighbors) {
                inDegree.compute(neighborScc, (k, v) -> v == null ? 1 : v + 1);
            }
        }

        // 2. Initialize Queue (Starts with components having in-degree 0)
        Queue<List<String>> queue = new LinkedList<>();
        for (List<String> scc : allSccs) {
            if (inDegree.get(scc) == 0) {
                queue.offer(scc);
                kahnPushes++; // Instrumentation: Queue Push
            }
        }

        // 3. Process Queue
        List<List<String>> sortedOrder = new ArrayList<>();
        while (!queue.isEmpty()) {
            List<String> uScc = queue.poll();
            kahnPops++; // Instrumentation: Queue Pop
            sortedOrder.add(uScc);

            // Decrease in-degree of all neighbors (dependencies)
            for (List<String> vScc : condAdj.getOrDefault(uScc, Collections.emptySet())) {
                inDegree.compute(vScc, (k, v) -> v - 1);

                // If neighbor's in-degree drops to 0, add to queue
                if (inDegree.get(vScc) == 0) {
                    queue.offer(vScc);
                    kahnPushes++; // Instrumentation: Queue Push
                }
            }
        }

        if (sortedOrder.size() != allSccs.size()) {
            // This should not happen in a DAG, but indicates a cycle if the graph wasn't a DAG.
            System.err.println("Warning: Topological sort resulted in an incomplete order. The graph might contain residual cycles.");
        }

        return sortedOrder;
    }

    // --- Main Execution ---

    public static void main(String[] args) {
        long startTime = 0;

        System.out.println("--- Task 1.2: Topological Sort of Condensation DAG (Kahn's Algorithm) ---");

        try {
            startTime = System.nanoTime();

            // Reused preparation logic
            Set<String> allNodes = new HashSet<>();
            GraphData[] dataHolder = new GraphData[1];
            Map<String, List<String>> G = loadGraph(GRAPH_FILE_NAME, allNodes, dataHolder);

            // 1. Find SCCs and build Condensation DAG
            List<List<String>> sccs = findSCCs(G, allNodes);
            Map<List<String>, String> sccIdMap = new HashMap<>();
            Map<List<String>, Set<List<String>>> condAdj = buildCondensationGraph(G, sccs, sccIdMap);

            // 2. Compute Topological Sort
            List<List<String>> componentOrder = topologicalSortKahn(condAdj, sccs);

            long endTime = System.nanoTime();
            double durationMillis = (endTime - startTime) / 1_000_000.0;

            //  Results


            String orderString = componentOrder.stream()
                    .map(sccIdMap::get)
                    .collect(Collectors.joining(" -> "));

            System.out.println("\nValid Topological Order of Components:");
            System.out.println(orderString);


            System.out.println("\n Derived Order of Original Tasks (Tasks sorted within components for clarity):");
            List<String> taskOrder = new ArrayList<>();
            for (List<String> scc : componentOrder) {

                List<String> sortedScc = new ArrayList<>(scc);
                Collections.sort(sortedScc);
                taskOrder.addAll(sortedScc);
            }
            System.out.println(String.join(", ", taskOrder));

            // --- Instrumentation Report ---
            System.out.println("\n--- Instrumentation Report ---");
            System.out.printf("Core Kahn Operations (Pushes/Pops): %d\n", kahnPushes + kahnPops);
            System.out.printf("Total Queue Pushes: %d\n", kahnPushes);
            System.out.printf("Total Queue Pops: %d\n", kahnPops);
            System.out.printf("Total Execution Time (Load, SCC, DAG, Sort): %.3f milliseconds\n", durationMillis);
            System.out.println("------------------------------");

        } catch (RuntimeException e) {
            System.err.println("\n❌ A critical error occurred: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("\n❌ An unexpected error occurred during execution.");
            e.printStackTrace();
        }
    }
}

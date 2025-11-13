package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Kosaraju's Algorithm for Strongly Connected Components (SCCs).
 * This simplified version combines graph loading, SCC logic, and metrics
 * into a single class for brevity.
 */
public class KosarajuSCC {

    // Change the JSON file name HERE  HERE HERE HERE
    private static final String GRAPH_FILE_NAME = "large3.json";
    // HERE


    private static long dfsVisits = 0;
    private static long dfsEdges = 0;

    // Internal class to map the JSON structure
    static class GraphData {
        public List<String> nodes;
        public List<List<String>> edges;
        public GraphData() {}
    }



    // Load graph data
    private static Map<String, List<String>> loadGraph(String jsonFileName, Set<String> allNodes, GraphData[] dataHolder) throws Exception {
        InputStream is = KosarajuSCC.class.getClassLoader().getResourceAsStream(jsonFileName);

        if (is == null) {
            throw new RuntimeException("Error: File not found in resources: " + jsonFileName);
        }

        ObjectMapper mapper = new ObjectMapper();
        GraphData data = mapper.readValue(new InputStreamReader(is), GraphData.class);
        dataHolder[0] = data;

        Map<String, List<String>> G = new HashMap<>();

        // Initialize all nodes and build G
        for (String node : data.nodes) {
            G.put(node, new ArrayList<>());
            allNodes.add(node);
        }
        for (List<String> edge : data.edges) {
            if (edge.size() == 2) {
                String u = edge.get(0);
                String v = edge.get(1);
                G.get(u).add(v);
                allNodes.add(u);
                allNodes.add(v);
            }
        }
        return G;
    }

    // Computes the transpose graph G^T
    private static Map<String, List<String>> getTransposeGraph(Map<String, List<String>> G, Set<String> allNodes) {
        Map<String, List<String>> GT = new HashMap<>();
        for (String node : allNodes) {
            GT.put(node, new ArrayList<>());
        }

        for (Map.Entry<String, List<String>> entry : G.entrySet()) {
            String u = entry.getKey();
            for (String v : entry.getValue()) {
                GT.get(v).add(u); // Reverse edge v to u in G^T
            }
        }
        return GT;
    }

    /** DFS Pass 1 **/
    private static void dfsPass1(String u, Map<String, List<String>> G, Set<String> visited, Stack<String> stack) {
        visited.add(u);
        dfsVisits++;

        for (String v : G.getOrDefault(u, Collections.emptyList())) {
            dfsEdges++;
            if (!visited.contains(v)) {
                dfsPass1(v, G, visited, stack);
            }
        }
        stack.push(u);
    }

    /** DFS Pass 2*/
    private static void dfsPass2(String u, Map<String, List<String>> GT, Set<String> visited, List<String> currentScc) {
        visited.add(u);
        dfsVisits++;
        currentScc.add(u);

        for (String v : GT.getOrDefault(u, Collections.emptyList())) {
            dfsEdges++;
            if (!visited.contains(v)) {
                dfsPass2(v, GT, visited, currentScc);
            }
        }
    }

    /** Build  DAG */
    private static Map<List<String>, Set<List<String>>> buildCondensationGraph(Map<String, List<String>> G, List<List<String>> sccs) {
        Map<List<String>, Set<List<String>>> condensationGraph = new LinkedHashMap<>();
        Map<String, List<String>> nodeToSccMap = new HashMap<>();


        for (List<String> scc : sccs) {
            condensationGraph.put(scc, new HashSet<>());
            for (String node : scc) {
                nodeToSccMap.put(node, scc);
            }
        }


        for (List<String> sccU : sccs) {
            Set<List<String>> neighbors = condensationGraph.get(sccU);
            for (String u : sccU) {

                for (String v : G.getOrDefault(u, Collections.emptyList())) {
                    List<String> sccV = nodeToSccMap.get(v);


                    if (sccU != sccV) {
                        neighbors.add(sccV);
                    }
                }
            }
        }
        return condensationGraph;
    }


    public static void main(String[] args) {
        long startTime = 0;

        System.out.println("--- Kosaraju's Algorithm for Strongly Connected Components (SCCs) ---");

        try {
            startTime = System.nanoTime(); //start time

            Set<String> allNodes = new HashSet<>();
            GraphData[] dataHolder = new GraphData[1];


            Map<String, List<String>> G = loadGraph(GRAPH_FILE_NAME, allNodes, dataHolder);
            Map<String, List<String>> GT = getTransposeGraph(G, allNodes);

            // DFS Pass 1
            Stack<String> orderStack = new Stack<>();
            Set<String> visited = new HashSet<>();
            for (String node : allNodes) {
                if (!visited.contains(node)) {
                    dfsPass1(node, G, visited, orderStack);
                }
            }

            // DFS Pass 2
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


            Map<List<String>, Set<List<String>>> condensationGraph = buildCondensationGraph(G, sccs);

            long endTime = System.nanoTime(); // Stop time measurement
            double durationMillis = (endTime - startTime) / 1_000_000.0;


            //RESULTS


            System.out.println("\nStrongly Connected Components Found:");
            Map<List<String>, String> sccIdMap = new HashMap<>();

            for (int i = 0; i < sccs.size(); i++) {
                List<String> component = sccs.get(i);
                Collections.sort(component);
                String sccId = "SCC " + (i + 1);
                sccIdMap.put(component, sccId);
                System.out.printf("%s (Size: %d): %s\n", sccId, component.size(), component);
            }

            System.out.println("\nTotal SCCs: " + sccs.size());

            System.out.println("\n--- Condensation Graph (DAG) Edges ---");
            for (List<String> sccFrom : sccs) {
                String sccFromId = sccIdMap.get(sccFrom);
                String edgesTo = condensationGraph.get(sccFrom).stream()
                        .map(sccIdMap::get)
                        .collect(Collectors.joining(", "));

                System.out.printf("  %s -> [%s]\n", sccFromId, edgesTo.isEmpty() ? "None" : edgesTo);
            }

            //Instrumentation
            System.out.println("\n--- Instrumentation Report  ---");
            System.out.printf("Total DFS Visits (Nodes): %d\n", dfsVisits);
            System.out.printf("Total DFS Edges Traversed: %d\n", dfsEdges);
            System.out.printf("Total Execution Time: %.3f milliseconds\n", durationMillis);
            System.out.println("------------------------------------------");

        } catch (RuntimeException e) {
            System.err.println("\nA critical error occurred: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("\nAn unexpected error occurred during execution.");
            e.printStackTrace();
        }
    }
}

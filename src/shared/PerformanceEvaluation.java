package shared;

import app_kvServer.KVServer;
import client.KVStore;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import app_kvECS.ECSClient;
import shared.messages.IKVMessage.StatusType;

public class PerformanceEvaluation {

    private ECSClient ecs;
    private List<KVServer> servers;
    private List<KVStore> clients;
    private List<String> datasetKeys;
    private List<String> datasetVals;

    public PerformanceEvaluation() {
        ecs = new ECSClient("localhost", 3000);

        datasetKeys = new ArrayList<String>();
        datasetVals = new ArrayList<String>();

    }

    /**
     * Run experiment for performance eval
     * 
     * @param numServers number of servers to start
     * @param numClients number of clients to start
     * @param numPuts number of puts to perform
     * @param numGets number of gets to perform
     * @return string summary of experiment
     * @throws Exception 
     */
    public String experiment(int numServers, int numClients, int numPuts, int numGets, int incPort) throws Exception {

        servers = new ArrayList<KVServer>();
        clients = new ArrayList<KVStore>();

        StringBuilder sb = new StringBuilder();
        sb.append("Performance evaluation summary\n");
        sb.append("------------------------------\n");
        sb.append(numServers + " servers\n");
        sb.append(numClients + " clients\n");
        sb.append(numPuts + " puts\n");
        sb.append(numGets + " gets\n");

        // start from a clean storage every time

        Process proc = Runtime.getRuntime().exec("rm -rf serverdata");                        
        proc.waitFor();

        System.out.println("Starting servers");
        long start = System.currentTimeMillis();
        for (int i = 0; i < numServers; i++) {
            servers.add(new KVServer(3001 + i + incPort, 0, "None"));
            servers.get(i).ecsAddress = "localhost";
            servers.get(i).ecsPort = 3000;
            servers.get(i).dataPath = "./serverdata/server" + i + "data/storage.json";
            servers.get(i).replica1DataPath = "./serverdata/server" + i + "data/storage_replica_1.json";
            servers.get(i).replica2DataPath = "./serverdata/server" + i + "data/storage_replica_2.json";
            servers.get(i).start();
        }
        ecs.awaitNodes(numServers, 50000);

        long elapsed = System.currentTimeMillis() - start;
        sb.append("Server startup time (ms): " + elapsed);

        Thread.sleep(60000); // wait for rebalance

        System.out.println("Starting up kv stores");
        start = System.currentTimeMillis();
        for (int i = 0; i < numClients; i++) {
            clients.add(
                new KVStore(servers.get(0).getHostname(), servers.get(0).getPort()));
            clients.get(i).connect();
        }
        elapsed = System.currentTimeMillis() - start;
        sb.append("\nClient startup time (ms): " + elapsed);

        Thread.sleep(30000); // wait for rebalance

        if (numPuts == 0) {
            int j = 0;
            while (j < 200) {
                try {
                    clients.get(0).put(datasetKeys.get(j), datasetVals.get(j));
                } catch (NullPointerException e) {}
                j += 1;
            }
        }

        System.out.println("Starting put experiment");

        start = System.currentTimeMillis();
        int j = 0;
        while (j < numPuts) {
            for (KVStore client: clients) {
                try {
                    client.put(datasetKeys.get(j), datasetVals.get(j));
                } catch (Exception e) {}
                j += 1;
            }
        }
        System.out.println("Starting get experiment");
        j = 0;
        while (j < numGets) {
            for (KVStore client: clients) {
                try {
                    client.get(datasetKeys.get(j));
                } catch (Exception e) {}
                j += 1;
            }
        }
        elapsed = System.currentTimeMillis() - start;
        sb.append("\nTest put/get time (ms): " + elapsed);

        for (int i = 0; i < numClients; i++) {
            clients.get(i).disconnect();
        }

        ecs.shutdown();

        Thread.sleep(30000); // wait for shutdown
        return sb.toString();
    }

    /**
     * Create key value pairs from Enron dataset
     */
    public void createDataset() throws Exception {

        System.out.println("Creating dataset...");

        Path path = Paths.get("./maildir");

        Files.walkFileTree(path, new SimpleFileVisitor<Path>(){
            @Override
            public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs)
                throws IOException
            {
                String path = filePath.toString();

                try {
                    datasetKeys.add(path);
                    String text = new String(Files.readAllBytes(filePath));
                    text = text.replaceAll("\\n", ""); // get rid of all newlines
                    text = text.replaceAll("\\r", ""); // get rid of all carriage returns
                    datasetVals.add(text);
                } catch (IOException e) {
                    System.out.println("Error while reading " + path);
                }

                return FileVisitResult.CONTINUE;
            }
        });
        System.out.println("Dataset created!");
    }

    /**
     * Run experiment for milestone 1
     */
    public void MS1Experiment() {
        // eightyPutTwentyGet();
        // eightyGetTwentyPut();
        // fiftyPutFiftyGet();
    }

    /**
     * Run experiment for milestone 2
     */
    public void MS2Experiment() {
        List<String> results = new ArrayList<String>();
        try {
            createDataset();

            int incPort = 0;
            for (int i: new int[] {100}) {
                for (int j : new int[] {100}) {
                    String result;
                    result = experiment(i, j, 200, 0, incPort);
                    System.out.println(result);
                    incPort += i;
                    result = experiment(i, j, 100, 100, incPort);
                    System.out.println(result);
                    incPort += i;
                    result = experiment(i, j, 0, 200, incPort);
                    incPort += i;
                    System.out.println(result);
                }
            }
            for (String result: results) {
                System.out.println(result);
            }
            System.exit(0);
        } catch (Exception e) {
            for (String result: results) {
                System.out.println(result);
            }
            e.printStackTrace();
            System.exit(1);
        }
        
    }

    public static void main(String[] args) {
        PerformanceEvaluation pe = new PerformanceEvaluation();
        if (args[0].equals("-ms1")) {
            pe.MS1Experiment();
        } if (args[0].equals("-ms2")) {
            pe.MS2Experiment();
        }
    }
}

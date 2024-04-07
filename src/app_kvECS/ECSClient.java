package app_kvECS;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import app_kvClient.KVClient;
import app_kvServer.KVServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.ServerError;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import ecs.IECSNode;
import ecs.ECSNode;
import shared.Hash;
import shared.messages.CommProtocol;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

public class ECSClient implements IECSClient {

    private static Logger logger = Logger.getRootLogger();

    private String address;
    private int port;

    private Map<String, IECSNode> nodes;
    private List<BigInteger> nodePositions;
    private Stack<ECSNode> availableServers;

    private ECSServerSocket ecsServerSocket;
    private ECSHeartbeatHandler heartbeatHandler;

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    public CountDownLatch latch;

    public ECSClient(String address, int port) {
        this.address = address;
        this.port = port;

        nodes = new HashMap<String, IECSNode>();
        nodePositions = new ArrayList<BigInteger>();
        availableServers = new Stack<ECSNode>();

        ecsServerSocket = new ECSServerSocket(this, address, port);
        new Thread(ecsServerSocket).start();

        heartbeatHandler = new ECSHeartbeatHandler(this);
        new Thread(heartbeatHandler).start();

    }

    public void close() {
        logger.info("Closing ECS Server");
        ecsServerSocket.setRunning(false);
        ecsServerSocket = null;
        // for (Map.Entry<String, IECSNode> node : nodes.entrySet()) {
        // node.getValue().tearDownConnection();
        // }
    }

    /**
     * Starts the storage service by calling start() on all KVServer
     * instances that participate in the service.
     *
     * @throws Exception some meaningfull exception on failure
     * @return true on success, false on failure
     */
    @Override
    public boolean start() {
        boolean allStarted = true;

        if (nodes == null || nodes.isEmpty()) {
            System.out.println("No nodes available to start.");
            return false;
        }

        r.lock();

        try {

            for (Map.Entry<String, IECSNode> node : nodes.entrySet()) {
                allStarted &= startNode(node.getValue());
            }

        } finally {
            r.unlock();
        }

        return allStarted;
    }

    /**
     * Starts one node.
     * 
     * @param node node to start
     * @return true on success, false on failure
     */
    private boolean startNode(IECSNode node) {
        if (!node.isStarted()) {
            logger.info("Starting " + node.getNodeName());
            node.serverStart();
            node.setStarted(true);
            // KVMessage res = node.serverStart();
            // if (res == null) {
            // logger.error("Failed to communicate with " + node.getNodeName());
            // return false;
            // } else {
            // node.setStarted(true);
            // }
        }
        logger.info("Successfully started " + node.getNodeName());
        return true;
    }

    /**
     * Stops the service; all participating KVServers are stopped
     * for processing client requests but the processes remain running.
     *
     * @throws Exception some meaningfull exception on failure
     * @return true on success, false on failure
     */
    @Override
    public boolean stop() {
        boolean allStopped = true;

        if (nodes == null || nodes.isEmpty()) {
            System.out.println("No nodes available to stop.");
            return false;
        }

        r.lock();
        try {

            for (Map.Entry<String, IECSNode> node : nodes.entrySet()) {
                stopNode(node.getValue());
            }

        } finally {
            r.unlock();
        }

        return allStopped;
    }

    /**
     * Stops one node.
     * 
     * @param node Node to stop
     * @return true on success, false on failure
     */
    private boolean stopNode(IECSNode node) {
        boolean stopped = true;
        if (node.isStarted()) {
            logger.info("Stopping " + node.getNodeName());
            node.serverStop();
            node.setStarted(false);
            // KVMessage res = node.serverStop();
            // if (res == null) {
            // stopped = false;
            // logger.error("Failed to communicate with " + node.getNodeName());
            // } else {
            // node.setStarted(false);
            // }
        }
        return stopped;
    }

    /**
     * Stops all server instances and exits the remote processes.
     *
     * @throws Exception some meaningful exception on failure
     * @return true on success, false on failure
     */
    @Override
    public boolean shutdown() {
        boolean allShutdown = true;

        if (nodes == null || nodes.isEmpty()) {
            System.out.println("No nodes available to stop.");
            return false;
        }

        w.lock();
        try {

            for (Map.Entry<String, IECSNode> node : nodes.entrySet()) {
                if (stopNode(node.getValue())) {
                    logger.info("Shutting down " + node.getKey());
                    node.getValue().serverShutdown();
                    removeHashRange(node.getValue());
                    // KVMessage res = node.getValue().serverShutdown();
                    // if (res == null) {
                    // logger.error("Failed to communicate with " + node.getKey());
                    // } else if (res.getStatus() == StatusType.SERVER_SHUTDOWN_ERROR) {
                    // allShutdown = false;
                    // logger.error(
                    // "Server " + node.getKey() + " failed to shutdown.");
                    // } else {
                    // // nodes.remove(node.getKey());
                    // removeHashRange(node.getValue());
                    // }
                } else {
                    logger.error("Node failed to stop, skipping.");
                }
            }

            nodes.clear();

        } finally {
            w.unlock();
        }

        return allShutdown;
    }

    /**
     * Add an available KVServer to the storage service at an
     * arbitrary position
     *
     * @return name of new server
     */
    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        IECSNode node;
        if (availableServers.isEmpty()) {
            logger.error("There are no servers available to add!");
            return null;
        }
        w.lock();
        try {
            node = availableServers.pop();

            logger.info("Adding " + node.getNodeName());
            nodes.put(node.getNodeName(), node);

            // start node
            startNode(node);

            // do this after put! uses nodes.size()
            Set<IECSNode> toRebalance = addHashRange(node);

            updateHashranges();

            for (IECSNode rebalNode : toRebalance) {
                logger.info("Rebalancing node " + rebalNode.getNodeName());
                rebalanceNode(rebalNode);
            }
        } finally {
            w.unlock();
        }

        return node;
    }

    /**
     * calls addNode
     *
     * @return set of strings containing the names of the nodes
     */
    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        Set<IECSNode> nodes = new HashSet<IECSNode>();

        for (int i = 0; i < count; i++) {
            nodes.add(addNode(cacheStrategy, cacheSize));

        }

        return nodes;
    }

    /**
     * Sets up `count` servers with the ECS
     *
     * @return array of strings, containing unique names of servers
     */
    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // interface not used
        return null;
    }

    /**
     * Wait for all nodes to report status or until timeout expires
     *
     * @param count   number of nodes to wait for
     * @param timeout the timeout in milliseconds
     * @return true if all nodes reported successfully, false otherwise
     */
    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        long startTime = System.currentTimeMillis();

        Set<IECSNode> reported = new HashSet<IECSNode>();

        while (System.currentTimeMillis() - startTime < timeout) {
            r.lock();
            try {
                for (IECSNode node : nodes.values()) {
                    if (node.getNodeStatus() != null) {
                        reported.add(node);
                    }
                }
            } catch (Exception e) {
                logger.error("Error: ", e);
            } finally {
                r.unlock();
            }

            if (reported.size() == count)
                return true;
        }

        return false;
    }

    /**
     * Removes nodes with names matching the nodeNames array
     *
     * @param nodeNames names of nodes to remove
     * @return true on success, false otherwise
     */
    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        boolean rebalanceSuccess = true;
        w.lock();
        try {
            for (String name : nodeNames) {

                IECSNode node = nodes.get(name);

                if (node == null) {
                    logger.error("Node " + name + " doesn't exist in storage service");
                    continue;
                }

                nodes.remove(name);

                removeHashRange(node);

                // update hash ranges of all nodes
                updateHashranges();

                // update hash range of just the node to be deleted
                // if (!nodes.isEmpty()) {
                //     String metadata = buildMetadataString();
                //     node.sendMessage(new KVMessage("KEYRANGE_UPDATE " + metadata));
                // }

                // rebalanceSuccess &= rebalanceNode(node);
                // stopNode(node);
            }
        } finally {
            w.unlock();
        }

        return rebalanceSuccess;

    }

    public void updateHashranges() {
        String metadata = buildMetadataString();
        for (IECSNode node : nodes.values()) {
            node.sendMessage(new KVMessage("KEYRANGE_UPDATE " + metadata));
        }
    }

    /**
     * Get a map of all nodes
     */
    @Override
    public Map<String, IECSNode> getNodes() {
        return nodes;
    }

    /**
     * Get the specific node responsible for the given key
     */
    @Override
    public IECSNode getNodeByKey(String Key) {
        IECSNode responsibleNode = null;

        for (Map.Entry<String, IECSNode> node : nodes.entrySet()) {
            String[] hashRange = node.getValue().getNodeHashRange();
            BigInteger hashRangeStart = new BigInteger(hashRange[0], 16);
            BigInteger hashRangeEnd = new BigInteger(hashRange[0], 16);

            if (Hash.inHashRange(Key, hashRangeStart, hashRangeEnd)) {
                responsibleNode = node.getValue();
            }
        }

        return responsibleNode;
    }

    /**
     * Creates an ECSNode for an available server and adds it to a
     * map of available servers.
     *
     * @param hostname hostname of the server
     * @param port     port of the server
     * @param socket   ECS-server socket
     */
    public void addServerToPool(String hostname, int port, Socket socket) {
        w.lock();
        try {
            availableServers.add(new ECSNode(hostname, port, socket));
        } finally {
            w.unlock();
        }
        addNode("None", 0);
    }

    /**
     * Logic for adding a new node to the hash ring and
     * updating the ranges of all affected nodes.
     * 
     * @param node node to add into the hash ring
     * @return set of nodes to rebalance
     */
    private Set<IECSNode> addHashRange(IECSNode node) {
        BigInteger position = node.getPosition();

        // first, add to positions
        nodePositions.add(position);
        Collections.sort(nodePositions);

        int n = nodePositions.size();

        Set<IECSNode> toRebalance = new HashSet<IECSNode>();

        // then recalculate hashranges
        for (int i = 0; i < n; i++) {
            BigInteger startRange;
            if (i == 0) {
                startRange = nodePositions.get(n - 1).add(BigInteger.ONE);
            } else {
                startRange = nodePositions.get(i - 1).add(BigInteger.ONE);
            }

            String[] hashRange = {
                    startRange.toString(16),
                    nodePositions.get(i).toString(16)
            };

            for (IECSNode toUpdate : nodes.values()) {
                String[] oldHashRange = toUpdate.getNodeHashRange();
                if (toUpdate.getPosition().compareTo(nodePositions.get(i)) == 0) {
                    if (oldHashRange == null) {
                        toUpdate.setNodeHashRange(hashRange);
                        toRebalance.add(toUpdate);
                    } else if (!oldHashRange[0].equals(hashRange[0])
                            || !oldHashRange[1].equals(hashRange[1])) {
                        toUpdate.setNodeHashRange(hashRange);
                        toRebalance.add(toUpdate);
                    }
                }
            }
        }

        return toRebalance;
    }

    /**
     * Logic for adding a new node to the hash ring and
     * updating the ranges of all affected nodes.
     * 
     * @param node node to add into the hash ring
     * @return set of nodes to rebalance
     */
    private void removeHashRange(IECSNode node) {
        BigInteger position = node.getPosition();

        int i = nodePositions.indexOf(position);
        nodePositions.remove(i);

        int n = nodePositions.size();

        if (n > 0) {
            BigInteger startRange;
            if (i == 0) {
                startRange = nodePositions.get(n - 1).add(BigInteger.ONE);
            } else {
                startRange = nodePositions.get(i - 1).add(BigInteger.ONE);
            }

            if (i == n) {
                i = 0;
            }

            String[] hashRange = {
                    startRange.toString(16),
                    nodePositions.get(i).toString(16)
            };

            for (IECSNode toUpdate : nodes.values()) {
                if (toUpdate.getPosition().compareTo(nodePositions.get(i)) == 0) {
                    toUpdate.setNodeHashRange(hashRange);
                }
            }
        }
    }

    /**
     * Sends a rebalance request to an ECSNode.
     * 
     * @param node node to rebalance
     * @return true on success, false on failure
     */
    private boolean rebalanceNode(IECSNode node) {
        // latch = new CountDownLatch(1); // awaiting node rebal
        node.rebalance();
        // try {
        //     latch.await();
        // } catch (InterruptedException e) {
        //     logger.error(e);
        // }
        // KVMessage res = node.rebalance(metadata);
        // if (res == null) {
        // logger.error(
        // "Failed to communicate with " + node.getNodeName());
        // return false;
        // } else if (res.getStatus() == StatusType.SERVER_STOPPED) {
        // logger.error(
        // "Server " + node.getNodeName() +
        // " is currently stopped, skipping rebalance.");
        // return false;
        // } else if (res.getStatus() == StatusType.REBALANCE_ERROR) {
        // logger.error(
        // "Server " + node.getNodeName() + " failed to rebalance.");
        // return false;
        // }
        return true;
    }

    /**
     * Serializes metadata in the format
     * KEYRANGE_START,KEYRANGE_END,HOST:PORT;...
     * 
     * @return string of metadata
     */
    private String buildMetadataString() {
        StringBuilder sb = new StringBuilder();

        for (IECSNode node : nodes.values()) {
            String[] hashRange = node.getNodeHashRange();
            String host = node.getNodeHost();
            int port = node.getNodePort();

            sb.append(
                    hashRange[0] + "," +
                            hashRange[1] + "," +
                            host + ":" + port + ";");
        }

        return sb.toString();
    }

    /**
     * 
     * @return
     */
    public Set<String> checkFailed() {
        Set<String> failNodes = new HashSet<String>();
        for (IECSNode node : nodes.values()) {
            if (!node.getHeartbeat()) {
                failNodes.add(node.getNodeName());
            }
            node.setHeartbeat(false);
        }
        return failNodes;
    }

    public static void main(String[] args) {
        Integer port = null;
        String address = "localhost";

        // Parse args
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-p": // Port number
                    try {
                        port = Integer.parseInt(args[i + 1]);
                    } catch (NumberFormatException nfe) {
                        System.out.println(
                                "Error: Invalid argument <port>! Not a number!");
                        System.exit(1);
                    }
                    break;
                case "-a": // Address
                    address = args[i + 1];
                    break;
                default:
                    break;
            }
        }

        if (port == null) {
            System.out.println("Error: argument <port> is required.");
            System.exit(1);
        }

        try {
            new LogSetup("logs/ecs.log", Level.ALL);
            ECSClient ecs = new ECSClient(address, port);

            ECSUserInterface cli = new ECSUserInterface(ecs);
            cli.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void proxyMain() {

        Thread ecsThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ECSClient ecs = new ECSClient("localhost", 8003);
                } catch (Exception e) {
                    System.exit(1);
                }
            }
        });

        ecsThread.start();

        KVServer server1 = new KVServer(8029, 0, null);
        KVServer server2 = new KVServer(8030, 0, null);
        KVServer server3 = new KVServer(8031, 0, null);

        server1.ecsAddress = "localhost";
        server2.ecsAddress = "localhost";
        server3.ecsAddress = "localhost";
        server1.ecsPort = 8003;
        server2.ecsPort = 8003;
        server3.ecsPort = 8003;

        server1.start();
        server2.start();
        server3.start();

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {

        }

        logger.info(server1.replica1);
        logger.info(server1.replica2);
        logger.info(server2.replica1);
        logger.info(server2.replica2);
        logger.info(server3.replica1);
        logger.info(server3.replica2);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {

        }

        server1.close();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {

        }
        server1 = null;
        server2.close();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {

        }
        server2 = null;
        server3.close();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {

        }
        server3 = null;

        try {
            ecsThread.join();
        } catch (InterruptedException e) {
            System.exit(1);
        }

    }
}

package app_kvServer;

import java.io.FileReader;
import java.io.FileWriter;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVStore;
import logger.LogSetup;
import shared.messages.KVMessage;
import shared.messages.CommProtocol;
import shared.messages.IKVMessage.StatusType;
import shared.Hash;

import static shared.messages.IKVMessage.StatusType.UNSUBSCRIBE;

public class KVServer extends Thread implements IKVServer {

	private static Logger logger = Logger.getRootLogger();

	private boolean running;
	private boolean startedByECS;
	private boolean startedBySelf;
	private boolean rebalancing;

	private Map<String, String> kvs;
	private Map<String, String> kvs_rep1;
	private Map<String, String> kvs_rep2;
	private Map<String, BigInteger[]> metadata;

	private int cacheSize;
	private String cacheStrategy;

	private int port;
	public String address = "localhost";

	private ServerSocket serverSocket;

	public Integer ecsPort;
	public String ecsAddress;
	private Socket ecsSocket;

	private KVServerHeartbeat heartbeat;

	public String dataPath = "./storage.json";
	public String replica1DataPath = "./storage_replica_1.json";
	public String replica2DataPath = "./storage_replica_2.json";

	private String coord1Addr; // kv server sending data stored in kvs_rep1
	private String coord2Addr; // kv server sending data stored in kvs_rep2

	public KVReplica replica1;
	public KVReplica replica2;

	private HashMap<String, String> subscribers;

	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	private final Lock r = rwl.readLock();
	private final Lock w = rwl.writeLock();

	/**
	 * Start KV Server at given port
	 * 
	 * @param port      given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *                  to keep in-memory
	 * @param strategy  specifies the cache replacement strategy in case the cache
	 *                  is full and there is a GET- or PUT-request on a key that is
	 *                  currently not contained in the cache. Options are "FIFO",
	 *                  "LRU",
	 *                  and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.cacheStrategy = strategy;

		// initialize empty hashmaps for KV caching
		kvs = new HashMap<String, String>();
		kvs_rep1 = new HashMap<String, String>();
		kvs_rep2 = new HashMap<String, String>();

		// initialize new metadata hashmap
		metadata = new HashMap<String, BigInteger[]>();
		subscribers = new HashMap<String, String>();

		// add shutdown hook to invoke close() on Ctrl+C
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				close();
			}
		});

	}

	/**
	 * Connects to the ECS immediately and spawns a new
	 * ClientConnection thread.
	 * 
	 * @return true if ECS successfully connected to
	 */
	private boolean contactECSStartup() {
		ecsSocket = null;
		OutputStream output = null;
		try {
			ecsSocket = new Socket(ecsAddress, ecsPort);
			ClientConnection ecsConnection = new ClientConnection(ecsSocket, this);
			new Thread(ecsConnection).start();

			logger.info("Connected to ECS at "
					+ ecsAddress + ":"
					+ ecsPort);

			output = ecsSocket.getOutputStream();

			CommProtocol.sendMessage(new KVMessage(
					"NEW_SERVER " + address + " " + port), output);

			heartbeat = new KVServerHeartbeat(ecsSocket, address, port);
			new Thread(heartbeat).start();

		} catch (UnknownHostException e) {
			logger.error("Unknown host: ", e);
			return false;
		} catch (IOException e) {
			logger.error("Error: ", e);
			return false;
		}

		return true;
	}

	/**
	 * Contacts the ECS to remove self from hash ring
	 * and rebalance keys on shutdown
	 * 
	 * @return true on success
	 */
	private boolean contactECSShutdown() {
		if (ecsSocket == null) {
			logger.info("No ECS connected");
			return false;
		}
		try {

			OutputStream output = ecsSocket.getOutputStream();

			CommProtocol.sendMessage(new KVMessage(
					"KILLING_MYSELF " + address + " " + port), output);

			startedByECS = false;
		} catch (Exception e) {
			logger.error("Error: ", e);
			return false;
		}
		return true;
	}

	/**
	 * Initializes server by opening the server socket, binding
	 * to the specified socket address, and initializing data
	 * from persisted (JSON) storage.
	 * 
	 * @return true if the server was successfully started up.
	 */
	private boolean initializeServer() {
		logger.info("Initializing server...");

		try {
			serverSocket = new ServerSocket();
			serverSocket.bind(new InetSocketAddress(address, port));
			logger.info("Server listening on port " + getPort());

			w.lock();
			try {
				initMapFromStorage();
			} finally {
				w.unlock();
			}

			if (ecsAddress != null && ecsPort != null) {
				contactECSStartup();
			} else {
				startedBySelf = true;
			}

		} catch (Exception e) {
			logger.error("Error: Cannot open server socket. ", e);
			if (e instanceof BindException) {
				logger.error("Port " + port + " is already bound.");
			}
			return false;
		}

		return true;
	}

	/**
	 * Gets whether KVServer is running or not.
	 * 
	 * @return true if KVServer is running.
	 */
	public boolean isRunning() {
		return running;
	}

	/**
	 * Sets whether KVServer is running or not.
	 * 
	 * @param run boolean to change running to.
	 */
	public void setRunning(boolean run) {
		running = run;
	}

	/**
	 * Get the port number of the server
	 * 
	 * @return port number
	 */
	@Override
	public int getPort() {
		return port;
	}

	/**
	 * Get the hostname of the server
	 * 
	 * @return hostname of server
	 */
	@Override
	public String getHostname() {
		return address;
	}

	/**
	 * Get the cache strategy of the server
	 * 
	 * @return cache strategy
	 */
	@Override
	public CacheStrategy getCacheStrategy() {
		try {
			return CacheStrategy.valueOf(this.cacheStrategy);
		} catch (IllegalArgumentException e) {
			logger.error("Invalid cache strategy.");
			return IKVServer.CacheStrategy.None;
		}
	}

	/**
	 * Get the cache size
	 * 
	 * @return cache size
	 */
	@Override
	public int getCacheSize() {
		return this.cacheSize;
	}

	/**
	 * Check if key is in cache.
	 * NOTE: does not modify any other properties
	 * 
	 * @param key to check if in cache.
	 * @return true if key in cache, false otherwise
	 */
	@Override
	public boolean inCache(String key) {
		return false;
	}

	/**
	 * Check if key is in storage.
	 * NOTE: does not modify any other properties
	 * 
	 * @return true if key in storage, false otherwise
	 */
	@Override
	public boolean inStorage(String key) {
		return kvs.containsKey(key) || kvs_rep1.containsKey(key) || kvs_rep2.containsKey(key);
	}

	/**
	 * Get the value associated with the key
	 * 
	 * @param key to get value of.
	 * @return value associated with key
	 * @throws Exception
	 *                   when key not in the key range of the server
	 */
	@Override
	public String getKV(String key) throws Exception {
		r.lock();
		try {
			if (kvs.containsKey(key)) {
				return kvs.get(key);
			} else if (kvs_rep1.containsKey(key)) {
				return kvs_rep1.get(key);
			} else if (kvs_rep2.containsKey(key)) {
				return kvs_rep2.get(key);
			} else {
				throw new Exception(
						"Key not in key range of server.");
			}
		} finally {
			r.unlock();
		}
	}

	/**
	 * Put the key-value pair into storage
	 * 
	 * @param key   to put into storage.
	 * @param value to associate with key in storage.
	 * @throws Exception
	 *                   when key not in the key range of the server
	 */
	@Override
	public void putKV(String key, String value) throws Exception {
		w.lock();
		try {
			if (key == null) {
				throw new Exception("Null Key!");
			} else if (value == null) {
				throw new Exception("Null Value!");
			}
			kvs.put(key, value);
			writeToStorage(kvs, dataPath);

			// checking if the key is subscribed to and notify all subscribers if so
			if (subscribers.containsKey(key)) {
				String[] subscriberInfo = subscribers.get(key).split(",");
				for (String subscriber : subscriberInfo) {
					String[] subscriberAddr = subscriber.split(":");
					CommProtocol.sendMessage(new KVMessage(
							StatusType.PUT_SUCCESS.name()
									+ " " + key
									+ " " + value), new Socket(subscriberAddr[0], Integer.parseInt(subscriberAddr[1])).getOutputStream());
				}
			}
		} finally {
			w.unlock();
		}
	}

	public void putKVReplica(Map<String, String> kvs, String key, String value)
			throws Exception {
		w.lock();
		try {
			if (key == null) {
				throw new Exception("Null Key!");
			} else if (value == null) {
				throw new Exception("Null Value!");
			}
			kvs.put(key, value);
			if (this.kvs_rep1 == kvs)
				writeToStorage(kvs, replica1DataPath);
			if (this.kvs_rep2 == kvs)
				writeToStorage(kvs, replica2DataPath);
		} finally {
			w.unlock();
		}
	}

	/**
	 * Clear the local cache of the server
	 */
	@Override
	public void clearCache() {
	}

	/**
	 * Clear the storage of the server
	 */
	@Override
	public void clearStorage() {
		w.lock();
		try {
			kvs.clear();
			writeToStorage(kvs, dataPath);
		} finally {
			w.unlock();
		}
	}

	/**
	 * Starts running the server
	 */
	@Override
	public void run() {
		setRunning(initializeServer());

		if (serverSocket != null) {
			while (isRunning()) {
				try {
					Socket client = serverSocket.accept();
					ClientConnection connection = new ClientConnection(client, this);
					new Thread(connection).start();

					logger.info("Connected to client at "
							+ client.getInetAddress().getHostName()
							+ ":" + client.getPort());
				} catch (SocketException e) {
					logger.info(
							"Server socket closed. Shutting down...");
				} catch (Exception e) {
					logger.error(
							"Error: Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	/**
	 * Abruptly stop the server without any additional actions
	 * NOTE: this includes performing saving to storage
	 */
	@Override
	public void kill() {
		logger.info("Killing server");
		if (serverSocket != null) {
			try {
				serverSocket.close();
			} catch (Exception e) {
				logger.error("Error: " +
						"Unable to close socket on port: " + port, e);
			}
		}
		setRunning(false);
	}

	/**
	 * Gracefully stop the server, lets ECS know that it is shutting down.
	 */
	@Override
	public void close() {
		logger.info("Closing server");
		contactECSShutdown();
		kvs_rep1.clear();
		writeToStorage(kvs_rep1, replica1DataPath);
		kvs_rep2.clear();
		writeToStorage(kvs_rep2, replica2DataPath);
		if (serverSocket != null) {
			try {
				serverSocket.close();
			} catch (Exception e) {
				logger.error("Error: " +
						"Unable to close socket on port: " + port, e);
			}
		}
		setRunning(false);
	}

	/**
	 * Writes to the JSON file, this could be to create the file
	 * or update the file.
	 * 
	 * This method is used to populate the coordinator or replica
	 * storage.
	 * 
	 * @param dataPath path to replica JSON file that this server houses
	 * @param kvs      hashmap of key value pairs to write to
	 */
	@Override
	public void writeToStorage(Map<String, String> kvs, String dataPath) {
		try {
			JSONObject contents = new JSONObject(kvs);
			FileWriter storage = new FileWriter(dataPath);
			storage.write(contents.toJSONString());
			storage.close();
			logger.info("Successfully wrote replicated stored values out to " + dataPath);
		} catch (IOException e) {
			logger.error("Error: ", e);
		}
	}

	/**
	 * initializes the hashmap/cache used while the application
	 * is running to hold (key, value) information
	 */
	@Override
	public void initMapFromStorage() {
		Path pathToFile = Paths.get(dataPath);
		if (!Files.exists(pathToFile)) {
			try {
				Files.createDirectories(pathToFile.getParent());
			} catch (IOException e) {
				logger.info("Error: " + e);
			}
		}

		JSONParser parser = new JSONParser();
		try {
			Object obj = parser.parse(new FileReader(dataPath));
			// A JSON object. Key value pairs are unordered.
			// JSONObject supports java.util.Map interface.
			JSONObject jsonObject = (JSONObject) obj;

			for (Object key : jsonObject.keySet()) {
				String keyStr = (String) key;
				String valStr = (String) jsonObject.get(keyStr);
				kvs.put(keyStr, valStr);
			}
		} catch (Exception e) {
			logger.info("Using new empty storage map.");
		}
	}

	/**
	 * Gets order of nodes by position in hash ring from metadata
	 * 
	 * @return an ArrayList of metadata entries in order
	 */
	public List<Map.Entry<String, BigInteger[]>> getNodeOrder() {
		List<Map.Entry<String, BigInteger[]>> nodePositions = new ArrayList<Map.Entry<String, BigInteger[]>>();
		for (Map.Entry<String, BigInteger[]> metadataEntry : metadata.entrySet()) {
			nodePositions.add(metadataEntry);
		}

		Comparator nodePosComparator = new Comparator<Map.Entry<String, BigInteger[]>>() {
			public int compare(
					Map.Entry<String, BigInteger[]> o1,
					Map.Entry<String, BigInteger[]> o2) {
				return o1.getValue()[1].compareTo(o2.getValue()[1]);
			}
		};

		Collections.sort(nodePositions, nodePosComparator);

		return nodePositions;
	}

	private void moveRepToMain(String failNodeAddr) {
		Map<String, String> kvsToRecover;
		if (coord1Addr == failNodeAddr) {
			logger.info("Recovering keys from Coordinator 1");
			kvsToRecover = kvs_rep1;
		} else if (coord2Addr == failNodeAddr) {
			logger.info("Recovering keys from Coordinator 2");
			kvsToRecover = kvs_rep2;
		} else {
			logger.error("Server not responsible for key recovery for " + failNodeAddr);
			return;
		}
		w.lock();
		try {
			for (Map.Entry<String, String> k : kvsToRecover.entrySet()) {
				logger.info("Putting " + k.getKey() + " into own storage");
				kvs.put(k.getKey(), k.getValue());
				if (replica1 != null) 
					replica1.putToReplica(k.getKey(), k.getValue());
				if (replica2 != null) 
					replica2.putToReplica(k.getKey(), k.getValue());
			}
			writeToStorage(kvs, dataPath);
		} finally {
			w.unlock();
		}
	}

	/**
	 * Find the positions of the nodes in the hash ring,
	 * thus finding this node's predecessors.
	 */
	public void updateCoordinators(
			List<Map.Entry<String, BigInteger[]>> newNodeOrder) {
		int n = newNodeOrder.size();

		String newCoord1Addr = null;
		String newCoord2Addr = null;

		if (n == 2) {
			for (int i = 0; i < n; i++) {
				if (newNodeOrder.get(i).getKey().equals(this.address + ":" + this.port)) {
					newCoord1Addr = newNodeOrder.get(((i - 1) + n) % n).getKey();
					break;
				}
			}
		} else if (n > 2) {
			for (int i = 0; i < n; i++) {
				if (newNodeOrder.get(i).getKey().equals(this.address + ":" + this.port)) {
					newCoord1Addr = newNodeOrder.get(((i - 1) + n) % n).getKey();
					newCoord2Addr = newNodeOrder.get(((i - 2) + n) % n).getKey();
					break;
				}
			}
		}

		logger.info("Old coordinator 1: " + coord1Addr);
		logger.info("Old coordinator 2: " + coord2Addr);
		logger.info("New coordinator 1: " + newCoord1Addr);
		logger.info("New coordinator 2: " + newCoord2Addr);

		// Move keys to main storage as part of node removal
		if (coord1Addr != null && !metadata.keySet().contains(coord1Addr)) {
			moveRepToMain(coord1Addr);
			// furthermore, if the new node order doesn't have 2nd coordinator as well
			if (coord2Addr != null && !metadata.keySet().contains(coord2Addr)) {
				moveRepToMain(coord2Addr);
			}
		} 

		// todo cleanup

		if (coord1Addr != null && coord1Addr.equals(newCoord2Addr)) {
			kvs_rep2 = new HashMap<String, String>(kvs_rep1);
			writeToStorage(kvs_rep2, replica2DataPath);
			kvs_rep1.clear();
			writeToStorage(kvs_rep1, replica1DataPath);
		} else if (newCoord1Addr == null || coord1Addr != null && !coord1Addr.equals(newCoord1Addr)) {
			kvs_rep1.clear(); // we have become the new replica of a new coordinator
			writeToStorage(kvs_rep1, replica1DataPath);
		}
		if (coord2Addr != null && coord2Addr.equals(newCoord1Addr)) {
			kvs_rep1 = new HashMap<String, String>(kvs_rep2);
			writeToStorage(kvs_rep1, replica1DataPath);			
			kvs_rep2.clear();
			writeToStorage(kvs_rep2, replica1DataPath);
		} else if (newCoord2Addr == null || coord2Addr != null && !coord2Addr.equals(newCoord2Addr)) {
			kvs_rep2.clear(); // we have become the new replica of a new coordinator
			writeToStorage(kvs_rep2, replica2DataPath);
		}

		coord1Addr = newCoord1Addr;
		coord2Addr = newCoord2Addr;

	}

	/**
	 * Finds the positions of nodes in the hash ring, thus
	 * finding this node's successors. Designates successors
	 * as replicas and either moves replica position or
	 * creates a new replica and copies its data over.
	 */
	public void updateReplicas(List<Map.Entry<String, BigInteger[]>> nodePositions) {

		// first, find new replica addresses
		String replica1Addr = null;
		String replica2Addr = null;

		int n = nodePositions.size();

		if (n == 2) {
			for (int i = 0; i < n; i++) {
				if (nodePositions.get(i).getKey().equals(this.address + ":" + this.port)) {
					replica1Addr = nodePositions.get((i + 1) % n).getKey();
					break;
				}
			}
		} else if (n > 2) {
			for (int i = 0; i < n; i++) {
				if (nodePositions.get(i).getKey().equals(this.address + ":" + this.port)) {
					replica1Addr = nodePositions.get((i + 1) % n).getKey();
					replica2Addr = nodePositions.get((i + 2) % n).getKey();
					break;
				}
			}
		}

		logger.info("Replica 1 addr: " + replica1Addr);
		logger.info("Replica 2 addr: " + replica2Addr);

		// temporarily store old replicas
		KVReplica tempReplica2 = replica2;
		KVReplica tempReplica1 = replica1;

		if (replica1Addr == null) {
			if (replica1 != null) {
				logger.info("Removing replica 1");
				replica1.disconnect();
				replica1 = null;
			}
		} else {
			if (tempReplica1 != null &&
					replica1Addr.equals(tempReplica1.hostname + ":" + tempReplica1.port)) {
				// do nothing
			} else if (tempReplica2 != null &&
					replica1Addr.equals(tempReplica2.hostname + ":" + tempReplica2.port)) {
				replica1 = tempReplica2;
				replica1.replicaNum = 1;
			} else {
				replica1 = new KVReplica(
						replica1Addr.split(":")[0],
						Integer.parseInt(replica1Addr.split(":")[1]),
						1);
				logger.info("Connecting to KVReplica 1");
				replica1.connect();
			}
		}
		if (replica2Addr == null) {
			if (replica2 != null) {
				logger.info("Removing replica 2");
				replica2.disconnect();
				replica2 = null;
			}
		} else {
			if (tempReplica2 != null &&
					replica2Addr.equals(tempReplica2.hostname + ":" + tempReplica2.port)) {
				// do nothing
			} else if (tempReplica1 != null &&
					replica2Addr.equals(tempReplica1.hostname + ":" + tempReplica1.port)) {
				replica2 = tempReplica1;
				replica2.replicaNum = 2;
			} else {
				replica2 = new KVReplica(
						replica2Addr.split(":")[0],
						Integer.parseInt(replica2Addr.split(":")[1]),
						2);
				logger.info("Connecting to KVReplica 2");
				replica2.connect();
			}

		}
	}

	/**
	 * Clears old metadata, parses newly received metadata
	 * string, and stores it in the metadata map.
	 * 
	 * Metadata string format:
	 * KEYRANGE_START,KEYRANGE_END,ADDR:PORT;
	 * 
	 * Metadata map format:
	 * {"ADDR:PORT", [KEYRANGE_START, KEYRANGE_END]}
	 * 
	 * @param newMetadata string of new metadata
	 */
	public void updateMetadata(String newMetadata) {
		metadata.clear();

		for (String server : newMetadata.split(";")) {
			String[] serverInfo = server.split(",");
			BigInteger keyrangeStart = new BigInteger(serverInfo[0], 16);
			BigInteger keyrangeEnd = new BigInteger(serverInfo[1], 16);
			String addrStr = serverInfo[2];

			metadata.put(
					addrStr, new BigInteger[] { keyrangeStart, keyrangeEnd });
		}

		logger.info("Updated metadata to: " + newMetadata);
	}

	/**
	 * Takes metadata map and puts it into the metadata
	 * string format:
	 * KEYRANGE_START,KEYRANGE_END,ADDR:PORT;...
	 *
	 * @return metadata string
	 */
	public String serializeMetadata() {
		StringBuilder metadataStr = new StringBuilder();
		r.lock();
		try {
			for (Map.Entry<String, BigInteger[]> server : metadata.entrySet()) {
				String addr = server.getKey();
				BigInteger[] kr = server.getValue();
				metadataStr.append(
						kr[0].toString(16) + "," +
								kr[1].toString(16) + "," +
								addr + ";");
			}
		} finally {
			r.unlock();
		}
		return metadataStr.toString();
	}

	 /**
	 * Takes metadata map and puts it into the metadata
	 * string format:
	 * KEYRANGE_START,KEYRANGE_END,ADDR:PORT;...
	 * 
	 * Where the key ranges are the valid read keyranges
	 * rather than write keyranges.
	  * 
	  * @return metadata string
	  */
	public String serializeReadMetadata() {
		StringBuilder metadataStr = new StringBuilder();
		r.lock();
		try {
			BigInteger[] krStart;
			List<Map.Entry<String, BigInteger[]>> nodePositions = getNodeOrder();
			int n = nodePositions.size();
			for (int i = 0; i < n; i++) {
				String addr = nodePositions.get(i).getKey();
				BigInteger[] kr = nodePositions.get(i).getValue();
				if (replica2 != null) {
					krStart = nodePositions.get((i - 2 + n) % n).getValue();
				} else if (replica1 != null) {
					krStart = nodePositions.get((i - 1 + n) % n).getValue();
				} else {
					krStart = kr;
				}
				metadataStr.append(
						krStart[0].toString(16) + "," +
								kr[1].toString(16) + "," +
								addr + ";");
			}
		} finally {
			r.unlock();
		}
		return metadataStr.toString();
	}

	/**
	 * Directs put requests to the right place.
	 * 
	 * @param kvs which set of kvs to modify: kvs, kvs_rep1, or kvs_rep2
	 * @param key key to put
	 * @param value value to put
	 * @return KVMessage with info about result
	 */
	private KVMessage putHandler(Map<String, String> kvs, String key, String value) {
		KVMessage res = null;
		try {
			boolean keyExists = false;
			r.lock();
			try {
				keyExists = kvs.containsKey(key);
			} finally {
				r.unlock();
			}
			if (value.equals("null")) {
				try {
					deleteKey(kvs, key, keyExists);
					res = new KVMessage(
							StatusType.DELETE_SUCCESS.name()
									+ " " + key);

					// checking if the key is subscribed to and notify all subscribers if so
					if (subscribers.containsKey(key)) {
						String[] subscriberInfo = subscribers.get(key).split(",");
						for (String subscriber : subscriberInfo) {
							String[] subscriberAddr = subscriber.split(":");
							CommProtocol.sendMessage(new KVMessage(
									StatusType.DELETE_SUCCESS.name()
											+ " " + key
											+ " " + value), new Socket(subscriberAddr[0], Integer.parseInt(subscriberAddr[1])).getOutputStream());
						}
						// since the key is deleted, remove it from the subscribers list? A key can be subscribed to without it existing though
						//subscribers.remove(key);
					}

				} catch (Exception e) {
					logger.error("Error: ", e);
					res = new KVMessage(
							StatusType.DELETE_ERROR.name()
									+ " " + key);
				}
			} else {
				if (this.kvs == kvs) {
					putKV(key, value);
				} else {
					putKVReplica(kvs, key, value);
				}
				if (keyExists) {
					res = new KVMessage(
							StatusType.PUT_UPDATE.name()
									+ " " + key
									+ " " + value);

					// checking if the key is subscribed to and notify all subscribers if so
					if (subscribers.containsKey(key)) {
						String[] subscriberInfo = subscribers.get(key).split(",");
						for (String subscriber : subscriberInfo) {
							String[] subscriberAddr = subscriber.split(":");
							CommProtocol.sendMessage(new KVMessage(
									StatusType.PUT_UPDATE.name()
											+ " " + key
											+ " " + value), new Socket(subscriberAddr[0], Integer.parseInt(subscriberAddr[1])).getOutputStream());
						}
					}

				} else {
					res = new KVMessage(
							StatusType.PUT_SUCCESS.name()
									+ " " + key
									+ " " + value);
				}
			}
		} catch (Exception e) {
			res = new KVMessage(
					StatusType.PUT_ERROR.name()
							+ " " + key
							+ " " + value);
			logger.error("Error: " + e);
		}
		return res;
	}

	/**
	 * Based on the hash ranges from the metadata, creates
	 * a mapping of keys from storage to servers for
	 * kv rebalancing
	 * 
	 * @return a map of servers with keys to send to them
	 */
	private Map<String, List<String>> createServerKeyBins() {
		Map<String, List<String>> serverKeys = new HashMap<String, List<String>>();

		r.lock();

		try {
			// build serverKeys map with empty lists
			for (String server : metadata.keySet()) {
				serverKeys.put(server, new ArrayList<String>());
			}

			for (String key : kvs.keySet()) {
				for (String server : metadata.keySet()) {
					// if keyHash in keyrange of server,
					// then add it to this server's bin

					BigInteger[] kr = metadata.get(server);

					if (Hash.inHashRange(key, kr[0], kr[1])) {
						serverKeys.get(server).add(key);
					}
				}
			}
		} finally {
			r.unlock();
		}

		return serverKeys;
	}

	/**
	 * Looks at input metadata string and distributes
	 * keys to other servers
	 *
	 * @throws Exception when rebalancing keys to another
	 * server fails
	 */
	public void rebalance() throws Exception {
		List<String> keysToRemove = new ArrayList<String>();

		w.lock();

		rebalancing = true;

		try {
			logger.info("Disconnecting from replicas temporarily");
			if (replica1 != null)
				replica1.disconnect();
			if (replica2 != null)
				replica2.disconnect();

			// map of which keys go to which servers
			Map<String, List<String>> serverKeys = createServerKeyBins();

			logger.info("New server keys: " + serverKeys);

			// go through each key
			// see which keys need to be sent to other servers
			for (String server : serverKeys.keySet()) {
				// parse out addr/port
				String targetAddr = server.split(":")[0];
				int targetPort = Integer.parseInt(
						server.split(":")[1]);

				// if target server is this server skip
				if (targetAddr.equals(this.address) &&
						targetPort == this.port) {
					continue;
				}

				// we want to act as client to another server
				if (!serverKeys.get(server).isEmpty()) {
					logger.info("Sending keys to " + targetAddr + ":" + targetPort);
					KVStore client = new KVStore(targetAddr, targetPort);
					client.connect();

					for (String k : serverKeys.get(server)) {
						KVMessage res = client.put(k, kvs.get(k));

						// checking if the key is subscribed to
						if (subscribers.containsKey(k)) {
							String[] subscriberInfo = subscribers.get(k).split(":");
							String subscriberAddr = subscriberInfo[0];
							int subscriberPort = Integer.parseInt(subscriberInfo[1]);
							KVStore subscriber = new KVStore(subscriberAddr, subscriberPort);
							subscriber.connect();
							subscriber.subscribe(k);
							subscriber.disconnect();
						}
						if (res.getStatus() == StatusType.PUT_ERROR) {
							client.disconnect();
							throw new Exception(
									"Put " + k + " to " + server + " failed!");
						}
						keysToRemove.add(k);
					}
					client.disconnect();
				}
			}
		} finally {

			rebalancing = false;

			// Don't delete keys right away; delete after
			for (String k : keysToRemove) {
				kvs.remove(k);
				// remove key from subscribers list
				subscribers.remove(k);
			}
			writeToStorage(kvs, dataPath);

			w.unlock();
		}

		if (replica1 != null) {
			replica1.connect();
			for (String k : keysToRemove) {
				replica1.putToReplica(k, "null");
			}
			logger.info("Sending memory to KVReplica 1");
			r.lock();
			try {
				replica1.copyMemoryToReplica(kvs);
			} finally {
				r.unlock();
			}

		}
		if (replica2 != null) {
			replica2.connect();
			for (String k : keysToRemove) {
				replica2.putToReplica(k, "null");
			}
			logger.info("Sending memory to KVReplica 2");
			r.lock();
			try {
				replica2.copyMemoryToReplica(kvs);
			} finally {
				r.unlock();
			}
		}
	}

	/**
	 * Deletes a key from storage.
	 *
	 * @param kvs       which storage (main, rep1, rep2) to deleted from
	 * @param key       key to delete
	 * @param keyExists whether or not key is already in storage
	 * @throws Exception when key doesn't exist
	 */
	private void deleteKey(Map<String, String> kvs, String key, boolean keyExists)
			throws Exception {
		if (!keyExists) {
			throw new Exception(
					"Key does not exist in storage and cannot be deleted.");
		}
		w.lock();
		try {
			kvs.remove(key);
			if (this.kvs == kvs)
				writeToStorage(kvs, dataPath);
			if (this.kvs_rep1 == kvs)
				writeToStorage(kvs, replica1DataPath);
			if (this.kvs_rep2 == kvs)
				writeToStorage(kvs, replica2DataPath);
		} finally {
			w.unlock();
		}
	}

		/**
	 * Handles incoming KVMessages with the correct actions and
	 * formulates responses to send back to the client.
	 *
	 * @param msg incoming KVMessage to handle.
	 * @return response KVMessage to send back to client.
	 */
	public KVMessage handleMessage(KVMessage msg) {
		StatusType status = msg.getStatus();
		String key = msg.getKey();
		String value = msg.getValue();

		KVMessage res = null;

		if (!(startedByECS || startedBySelf)
				&& status != StatusType.SERVER_START) {
			return new KVMessage(
					StatusType.SERVER_STOPPED.name());
		}

		switch (status) {
			case SERVER_START:
				startedByECS = true;
				res = new KVMessage(
						StatusType.SERVER_START_SUCCESS.name());
				break;
			case SERVER_STOP:
				startedByECS = false;
				res = new KVMessage(
						StatusType.SERVER_STOP_SUCCESS.name());
				break;
			case SERVER_SHUTDOWN:
				startedByECS = false;
				try {
					close();
					res = new KVMessage(
							StatusType.SERVER_SHUTDOWN_SUCCESS.name());
				} catch (Exception e) {
					res = new KVMessage(
							StatusType.SERVER_SHUTDOWN_ERROR.name());
				}
				break;
			case KEYRANGE_UPDATE:
				updateMetadata(key);
				List<Map.Entry<String, BigInteger[]>> newNodeOrder = getNodeOrder();
				updateReplicas(newNodeOrder);
				updateCoordinators(newNodeOrder);
				res = new KVMessage(
						StatusType.KEYRANGE_SUCCESS.name());
				break;
			case PUT_FROM_COORDINATOR_1:
				res = putHandler(kvs_rep1, key, value);
				break;
			case PUT_FROM_COORDINATOR_2:
				res = putHandler(kvs_rep2, key, value);
				break;
			case REBALANCE:
				try {
					rebalance();
					res = new KVMessage(
							StatusType.REBALANCE_SUCCESS.name());
				} catch (Exception e) {
					logger.error("Error: ", e);
					res = new KVMessage(
							StatusType.REBALANCE_ERROR.name());
				}
				break;
			case KEYRANGE:
				res = new KVMessage(
						StatusType.KEYRANGE_SUCCESS.name()
								+ " " + serializeMetadata());
				break;
			case KEYRANGE_READ:
				res = new KVMessage(
						StatusType.KEYRANGE_SUCCESS.name()
								+ " " + serializeReadMetadata());
				break;
			case PUT:
				BigInteger[] ownKeyrange = metadata.get(this.address + ":" + this.port);
				if (rebalancing) {
					res = new KVMessage(
							StatusType.SERVER_WRITE_LOCK.name());
				} else if (ownKeyrange != null
						&& !Hash.inHashRange(key, ownKeyrange[0], ownKeyrange[1])) {
					res = new KVMessage(
							StatusType.SERVER_NOT_RESPONSIBLE.name());
				} else {
					res = putHandler(kvs, key, value);
					if (replica1 != null)
						replica1.putToReplica(key, value);
					if (replica2 != null)
						replica2.putToReplica(key, value);
				}
				break;
			case GET:
				try {
					res = new KVMessage(
							StatusType.GET_SUCCESS.name()
									+ " " + key
									+ " " + getKV(key));
				} catch (Exception e) {
					// if there is no ECS return status should be GET_ERROR
					// otherwise it might be in keyrange of other server
					// OR - if in keyrange and doesnt exist, then return GET_ERROR

					BigInteger[] kr = metadata.get(this.address + ":" + this.port);
					boolean responsibleForKey = kr != null &&
							Hash.inHashRange(key, kr[0], kr[1]);
					if (startedBySelf || responsibleForKey) {
						res = new KVMessage(
								StatusType.GET_ERROR.name());
						logger.error("Error: " + e);
					} else {
						res = new KVMessage(
								StatusType.SERVER_NOT_RESPONSIBLE.name());
						logger.info(e);
					}
				}
				break;
			case SUBSCRIBE:
				try {
					if(kvs.containsKey(msg.getKey())) {
						StringBuilder subscribers_string = new StringBuilder();
						// if the key is not already subscribed to
						if(!subscribers.containsKey(msg.getKey())) {
							subscribers.put(msg.getKey(), subscribers_string.append(msg.getValue()).toString());
							res = new KVMessage(
									StatusType.SUBSCRIBE_SUCCESS.name());
						// if the key is subscribed to and the subscriber is not already a part of those subscribed
						} else if (subscribers.containsKey(msg.getKey()) && !subscribers.get(msg.getKey()).contains(msg.getValue())) {
							subscribers_string.append(subscribers.get(msg.getKey()))
									.append(",")
									.append(msg.getValue());
							subscribers.put(msg.getKey(), subscribers_string.toString());
							res = new KVMessage(
									"SUBSCRIPTION UPDATE: " + "Client " + msg.getValue() + " is now subscribed.");
						// if the key is subscribed to and the subscriber is already a part of those subscribed
						} else {
							res = new KVMessage(
									"SUBSCRIPTION UPDATE: " + "Client " + msg.getValue() + " is already subscribed.");
						}
					} else {
						res = new KVMessage(
								StatusType.SERVER_NOT_RESPONSIBLE.name());
					}
				}
				catch (Exception e) {
					logger.error("Subscription Error (SUBSCRIBE)", e);
					res = new KVMessage(
							"SUBSCRIBE Failure due to unknown subscription type.");
				}
				break;
			case UNSUBSCRIBE:
				try {
					if (kvs.containsKey(msg.getKey())) {
						// does not contain the key/ key is not being subscribed to
						if (!subscribers.containsKey(msg.getKey())) {
							res = new KVMessage(
									"UNSUBSCRIBE UPDATE: " + "Key " + msg.getKey() + " is not being subscribed to.");
						// contains the key and the client trying to unsubscribe is subscribed
						} else if (subscribers.containsKey(msg.getKey()) && subscribers.get(msg.getKey()).contains(msg.getValue())) {
							StringBuilder subscribers_string = new StringBuilder();
							String[] subscribers_list = subscribers.get(msg.getKey()).split(",");
							for (String subscriber : subscribers_list) {
								if (!subscriber.equals(msg.getValue())) {
									subscribers_string.append(subscriber).append(",");
								}
							}
							subscribers.put(msg.getKey(), subscribers_string.toString());
							res = new KVMessage(
									"UNSUBSCRIBE UPDATE: " + "Client " + msg.getValue() + " is now unsubscribed.");
						// contains the key but the client trying to unsubscribe is not subscribed
						} else {
							res = new KVMessage(
									"UNSUBSCRIBE UPDATE: " + "Client " + msg.getValue() + " is not subscribed.");
						}
					} else {
						res = new KVMessage(
								StatusType.SERVER_NOT_RESPONSIBLE.name());
					}
				} catch(Exception e){
					logger.error("Subscription Error (UNSUBSCRIBE)", e);
					res = new KVMessage(
							"UNSUBSCRIBE Failure due to unknown subscription type.");
				}
				break;
			default:
				res = new KVMessage(
						"FAILED Failure due to unknown status type.");
				break;

		}

		return res;
	}

	/**
	 * Main entry point for the server application.
	 * 
	 * @param args contains arguments denoted by flags
	 */
	public static void main(String[] args) {
		try {
			Integer port = null;
			String address = "localhost";
			String dataDir = ".";
			String logPath = "server.log";
			Level logLevel = Level.ALL;

			Integer ecsPort = null;
			String ecsAddress = null;

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
					case "-d": // data directory
						// assume format is like "path/to/something"
						// no slashes at end or beginning
						dataDir = args[i + 1];
						break;
					case "-a": // Address
						address = args[i + 1];
						break;
					case "-l": // Logfile relative path
						logPath = args[i + 1];
						break;
					case "-ll": // log level
						if (LogSetup.isValidLevel(args[i + 1])) {
							logLevel = Level.toLevel(args[i + 1]);
						} else {
							System.out.println(
									"Error: argument <loglevel> is not a valid log level.");
							System.out.println(
									"Possible log levels: " + LogSetup.getPossibleLogLevels());
							System.exit(1);
						}
						break;
					case "-b": // ECS server address
						try {
							String[] ecs = args[i + 1].split(":");
							if (ecs.length != 2) {
								System.out.println(
										"Error: Invalid argument, must be in the format <addr>:<port>!");
								System.exit(1);
							}

							ecsAddress = ecs[0];
							ecsPort = Integer.parseInt(ecs[1]);
						} catch (NumberFormatException nfe) {
							System.out.println(
									"Error: Invalid argument <port>! Not a number!");
							System.exit(1);
						}
						break;
					case "-h":
						StringBuilder sb = new StringBuilder();
						sb.append("SERVER APPLICATION HELP (Usage):\n");
						sb.append("::::::::::::::::::::::::::::::::");
						sb.append("::::::::::::::::::::::::::::::::\n");
						sb.append("-p <port>");
						sb.append("\t Sets the port number. <port>: integer\n");
						sb.append("-b <addr>:<port>");
						sb.append("\t Sets the address and port number of the ECS service, if one is available.");
						sb.append("<port>: integer");
						sb.append("-d <dataDir>");
						sb.append("\t changes the directory where data is persisted. ");
						sb.append("<dataDir>: path to a directory");
						sb.append("-l <logPath>");
						sb.append("\t changes the path of the logfile. <logPath>: path to file");
						sb.append("-ll <logLevel>");
						sb.append("\t changes the log level. <logLevel>: ");
						sb.append(LogSetup.getPossibleLogLevels());
						System.out.println(sb.toString());
						break;
					default:
						break;
				}
			}

			if (port == null) {
				System.out.println("Error: argument <port> is required.");
				System.exit(1);
			}

			new LogSetup(logPath, logLevel);
			KVServer kvServer = new KVServer(port, 0, "None");
			kvServer.dataPath = dataDir + "/storage.json";
			kvServer.replica1DataPath = dataDir + "/storage_replica_1.json";
			kvServer.replica2DataPath = dataDir + "/storage_replica_2.json";
			kvServer.address = address;
			kvServer.ecsAddress = ecsAddress;
			kvServer.ecsPort = ecsPort;

			kvServer.start();

		} catch (Exception e) {
			System.out.println("Error:");
			e.printStackTrace();
			System.exit(1);
		}
	}

}

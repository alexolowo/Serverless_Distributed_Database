package client;

import app_kvClient.KVClient;
import app_kvClient.IKVClient.SocketStatus;
import app_kvECS.ECSClient;
import ecs.IECSNode;
import shared.Hash;
import shared.messages.CommProtocol;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public class KVStore implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();

	private Set<KVClient> listeners;
	
	public boolean connected = false;
	
	private Socket clientSocket;
	private OutputStream output;
 	private InputStream input;

	public String address;
	public int port;

	public String serverSockAddr;
	public int serverSockPort;

	private Map<String, BigInteger[]> metadata;
	private Map<String, BigInteger[]> metadataRead;

	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;

		// add KVClient ui as listener
		listeners = new HashSet<KVClient>();

		// setup metadata assuming only 1 kvserver
		metadata = new HashMap<String, BigInteger[]>();
		metadata.put(
			address+":"+port, 
			new BigInteger[]{BigInteger.ZERO, BigInteger.ONE.shiftLeft(128)});
		metadataRead = new HashMap<String, BigInteger[]>(metadata);

		// setup server socket thread
		new Thread(new KVStoreServerSocket(this)).start();
	}

	/**
	 * Establishes a connection to the KV Server.
	 *
	 * @throws Exception
	 *             if connection could not be established.
	 */
	@Override
	public void connect() throws Exception {
		clientSocket = new Socket(address, port);
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
		logger.info(
			"Connection established to address "
			+ address + " and port " + port);
		connected = true;
	}

	/**
	 * Disconnects the client from the currently connected server.
	 */
	@Override
	public void disconnect() {
		logger.info("Attempting to close connection...");
		
		try {
			tearDownConnection();
			for (KVClient listener : listeners) {
				listener.handleStatus(SocketStatus.DISCONNECTED);
			}
		} catch (Exception e) {
			logger.error("Unable to close connection!", e);
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

		for (String server: newMetadata.split(";")) {
			String[] serverInfo = server.split(",");
			BigInteger keyrange_start = new BigInteger(serverInfo[0], 16);
			BigInteger keyrange_end = new BigInteger(serverInfo[1], 16);
			String addrStr = serverInfo[2];

			metadata.put(
					addrStr, new BigInteger[]{keyrange_start, keyrange_end});
		}

		logger.info("Updated metadata to: " + newMetadata);
	}

	/**
	 * 
	 * @param newMetadata
	 */
	public void updateReadMetadata(String newMetadata) {		
		metadataRead.clear();

		for (String server: newMetadata.split(";")) {
			String[] serverInfo = server.split(",");
			BigInteger keyrange_start = new BigInteger(serverInfo[0], 16);
			BigInteger keyrange_end = new BigInteger(serverInfo[1], 16);
			String addrStr = serverInfo[2];

			metadataRead.put(
					addrStr, new BigInteger[]{keyrange_start, keyrange_end});
		}

		logger.info("Updated metadata to: " + newMetadata);

	}

	/**
	 * 
	 * @param msgStr
	 * @return
	 */
	public KVMessage sendKVMessage(String msgStr) throws Exception {
		if (!connected) {
			throw new Exception("Not connected to a KV server.");
		}

		KVMessage res = null;

		try {
			CommProtocol.sendMessage(
				new KVMessage(msgStr), output);
		} catch (IOException e) {
			connectionLost();
		}

		try {
			res = CommProtocol.receiveMessage(input, true);
		} catch (IOException e) {
			connectionLost();
		}

		return res;
	}

	/**
	 * 
	 * @return
	 * @throws Exception
	 */
	public String keyrangeRead() throws Exception  {

		KVMessage res = sendKVMessage("KEYRANGE_READ");

		return res.getKey();
	}

	/**
	 * 
	 * @return
	 * @throws Exception
	 */
	public String keyrange() throws Exception  {
		
		KVMessage res = sendKVMessage("KEYRANGE");

		return res.getKey();
	}


	/**
	 * Inserts a key-value pair into the KVServer.
	 * Uses communication protocol to write a KV message to
	 * the output stream.
	 * Updates metadata if necessary.
	 * 
	 * @param key
	 *            the key that identifies the given value.
	 * @param value
	 *            the value that is indexed by the given key.
	 * @return a message that confirms the insertion of the tuple or an error.
	 * @throws Exception
	 *             if put command cannot be executed (e.g. not connected to any
	 *             KV server).
	 */
	@Override
	public KVMessage put(String key, String value) throws Exception {
		findResponsibleServer(key, metadata);
		
		KVMessage res = sendKVMessage("PUT " + key + " " + value);

		if (res.getStatus() == StatusType.SERVER_WRITE_LOCK) {
			// try to wait for a second
			Thread.sleep(1000);
			return put(key, value);
		}

		if (res.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
			String newMetadata = keyrange();
			updateMetadata(newMetadata);
			return put(key, value);
		}

		for (KVClient listener : listeners) {
			listener.handleNewMessage(res);
		}

		return res;

	}

	/**
	 * Retrieves the value for a given key from the KVServer.
	 *
	 * @param key the key that identifies the value.
	 * @return the value, which is indexed by the given key.
	 * @throws Exception if get command cannot be executed
	 * 		(e.g. not connected to any KV server).
	 */
	@Override
	public KVMessage get(String key) throws Exception {
		findResponsibleServer(key, metadataRead);
		
		KVMessage res = sendKVMessage("GET " + key);

		if (res.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
			String newMetadata = keyrangeRead();
			updateReadMetadata(newMetadata);
			return get(key);
		}
		
		return res;
	}

	/**
	 * Subscribes to changes in a given key that happen server-side.
	 *
	 * @param key the key
	 * @return SERVER_NOT_RESPONSIBLE or SUBSCRIBE_SUCCESS
	 * @throws Exception if subscribe command cannot be executed
	 * 		(e.g. not connected to any KV server).
	 */
	public KVMessage subscribe(String key) throws Exception {
		return subscribeAnyClient(key, serverSockAddr, serverSockPort);
	}

	/**
	 * Subscribes to changes in a given key that happen server-side.
	 *
	 * @param key the key
	 * @param addr addr of client to subscribe
	 * @param port port of client to subscribe
	 * @return SERVER_NOT_RESPONSIBLE or SUBSCRIBE_SUCCESS
	 * @throws Exception if subscribe command cannot be executed
	 * 		(e.g. not connected to any KV server).
	 */
	public KVMessage subscribeAnyClient(
			String key, String addr, int port) throws Exception {

		findResponsibleServer(key, metadata);
		
		KVMessage res = sendKVMessage("SUBSCRIBE " + key + " " + addr + ":" + port);

		if (res.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
			String newMetadata = keyrange();
			updateMetadata(newMetadata);
			return subscribeAnyClient(key, addr, port);
		}
		
		return res;
	}

	/**
	 * Unsubscribes to changes in a given key that happen server-side.
	 *
	 * @param key the key
	 * @return SERVER_NOT_RESPONSIBLE or UNSUBSCRIBE_SUCCESS
	 * @throws Exception if unsubscribe command cannot be executed
	 * 		(e.g. not connected to any KV server).
	 */
	public KVMessage unsubscribe(String key) throws Exception {
		return unsubscribeAnyClient(key, serverSockAddr, serverSockPort);
	}

	/**
	 * Unsubscribes to changes in a given key that happen server-side.
	 *
	 * @param key the key
	 * @param addr addr of client to unsubscribe
	 * @param port port of client to unsubscribe
	 * @return SERVER_NOT_RESPONSIBLE or UNSUBSCRIBE_SUCCESS
	 * @throws Exception if unsubscribe command cannot be executed
	 * 		(e.g. not connected to any KV server).
	 */
	public KVMessage unsubscribeAnyClient(
			String key, String addr, int port) throws Exception {
				
		findResponsibleServer(key, metadata);
		
		KVMessage res = sendKVMessage("UNSUBSCRIBE " + key + " " + addr + ":" + port);

		if (res.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
			String newMetadata = keyrange();
			updateMetadata(newMetadata);
			return unsubscribeAnyClient(key, addr, port);
		}
		
		return res;
	}

	/**
	 * Uses cached metadata to map key to send to the correct server
	 * and reconnects if the server isn't the currently connected one.
	 * 
	 * @param key
	 * @param metadata
	 * @throws Exception
	 */
	private void findResponsibleServer(String key, Map<String, BigInteger[]> metadata) 
			throws Exception {
		// find the server responsible for the key
		for(String node: metadata.keySet()){
		// get the key range from the metadata
			BigInteger krBeginning = metadata.get(node)[0];
			BigInteger krEnding = metadata.get(node)[1];
			if (Hash.inHashRange(key,krBeginning ,krEnding)){
				String newAddr = node.split(":")[0];
				int newPort = Integer.parseInt(node.split(":")[1]);
				if (!newAddr.equals(this.address) || newPort != this.port) {
					disconnect();
					this.address = newAddr;
					this.port = newPort;
					connect();
				} 
				break;
			}
		}
	}

	/**
	 * If lost connection detected, tear down the connection to
	 * the server.
	 */
	private void connectionLost() {
		logger.error("Connection lost!");

		try {
			tearDownConnection();
			for (KVClient listener : listeners) {
				listener.handleStatus(SocketStatus.CONNECTION_LOST);
			}
		} catch (Exception e) {
			logger.error("Unable to close connection!", e);
		}
	}

	/**
	 * Closes input and output streams and closes the client
	 * socket.
	 */
	private void tearDownConnection() {
		connected = false;
		logger.info("Tearing down the connection ...");
		if (clientSocket != null) {
			try {
				input.close();
				output.close();
				clientSocket.close();
			} catch (IOException e) {
				logger.error("Error: ", e);
			}
			clientSocket = null;
			logger.info("Connection closed.");
		}
	}

	/**
	 * Adds a KVClient (CLI) as a listener to handle
	 * received messages and statuses from this 
	 * instance of KVStore.
	 * 
	 * @param listener an instance of KVClient.
	 */
	public void addListener(KVClient listener) {
		listeners.add(listener);
	}
}


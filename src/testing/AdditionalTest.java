package testing;

import app_kvServer.KVServer;
import app_kvServer.KVServerHeartbeat;
import client.KVStore;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Test;

import app_kvECS.ECSClient;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.apache.log4j.Level;

import junit.framework.TestCase;

import java.beans.Transient;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import logger.LogSetup;
import shared.messages.CommProtocol;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;
import shared.Hash;

public class AdditionalTest extends TestCase {

	private static KVServer server;
	private static KVStore kvStore;
	private static boolean setUpDone = false;
	private static Thread ecsThread;

	private static int testsRun = 0;
	private int numTests = 500;

	private int ecsPort = 9998;
	private int serverPort = 3321; // Ensure this matches your test server

	@Override
	protected void setUp() {
		if (!setUpDone) {
			ecsThread = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						ECSClient ecs = new ECSClient("localhost", ecsPort);
					} catch (Exception e) {
						System.exit(1);
					}
				}
			});
			ecsThread.start();

			server = new KVServer(serverPort, 0, "None");
			server.start();
			kvStore = new KVStore("localhost", serverPort);
			try {
				kvStore.connect();
			} catch (Exception e) {
			}
			setUpDone = true;
		}
	}

	@Override
	protected void tearDown() {
		testsRun += 1;
		if (testsRun == numTests) {
			server.close();
			kvStore.disconnect();
			ecsThread.interrupt();
		}
	}

	@Test
	public void testStub() {
		assertTrue(true);
	}

	// KVMessage tests

	@Test
	public void testNormalMessage() {
		String msg = "put example 1 2 3 4 5";
		KVMessage testMsg = new KVMessage(msg);

		assertTrue(
				testMsg.getStatus() == StatusType.PUT &&
						testMsg.getKey().equals("example") &&
						testMsg.getValue().equals("1 2 3 4 5"));
	}

	@Test
	public void testArbitraryMessage() {
		String msg = "what a weird message";
		KVMessage testMsg = new KVMessage(msg);

		assertTrue(
				testMsg.getStatus() == null &&
						testMsg.getKey() == null &&
						testMsg.getValue().equals(msg));
	}

	// Server tests

	@Test
	public void testSetupServer() throws Exception {
		KVServer server2 = new KVServer(0, 0, "None");
		server2.dataPath = "storage2.json";
		server2.start();
		server2.putKV("test", "val");
		String val = server2.getKV("test");
		server2.close();

		assertEquals("val", val);
	}

	@Test
	public void testGetKVFailForKeyNotFound() {
		Exception ex = null;

		try {
			server.getKV("nonExistingKey");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex != null);// , "Should throw an exception for non-existing key"
	}

	@Test
	public void testPutKVSuccess() throws Exception {
		String key = "testKey";
		String value = "testValue";
		server.putKV(key, value);
		assertEquals(value, server.getKV(key)); // , "Value retrieved should match value inserted"
	}

	@Test
	public void testClearStorage() throws Exception {
		Exception ex = null;

		try {
			server.putKV("testKey1", "testValue1");
			server.clearStorage();
			server.getKV("testKey1");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex != null);// , "Storage should be empty after clear"
	}

	@Test
	public void testServerInitializationFailOnInvalidPort() {
		KVServer invalidServer = new KVServer(-1, 0, "None"); // Using an invalid port number
		assertFalse(invalidServer.isRunning());
	}

	@Test
	public void testWriteToStorage() throws Exception {
		server.clearStorage();
		server.putKV("testKey", "testValue");
		JSONParser parser = new JSONParser();
		// Now read the file to check if the data is written correctly
		// This assumes you have a method to read from the storage file
		JSONObject serverObj = (JSONObject) parser.parse(new FileReader(server.dataPath));

		// NOTE assumes serverJSON string will be formatted
		// with no spaces
		String myJSON = "{\"testKey\":\"testValue\"}";
		String serverJSON = serverObj.toString();

		// "Value should be written to storage file"
		assertEquals(myJSON, serverJSON);
	}

	@Test
	public void testInvalidKey() throws Exception {
		Exception ex = null;

		try {
			server.putKV(null, "value");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex != null); // "Should throw an exception for null key"
	}

	@Test
	public void testInvalidVal() throws Exception {
		Exception ex = null;

		try {
			server.putKV("key", null);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex != null); // , "Should throw an exception for null value"

	}

	@Test
	public void testServerShutdown() {
		server.kill();
		assertFalse(server.isRunning());
	}

	// Hash function

	@Test
	public void testHashFunction() {
		BigInteger keyHash = Hash.hash("I love ece419");
		BigInteger testHash = new BigInteger("0e9a4535b6b2e39602e5d6367c3aea60", 16);

		assertEquals(testHash.compareTo(keyHash), 0);
	}

	// KVServer Metadata Tests

	@Test
	public void testUpdateMetadata() {
		String testMetadata = "0," +
				"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF," +
				"localhost:1234;" +
				"0," +
				"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF," +
				"localhost:5432;";

		server.updateMetadata(testMetadata);
		String resMetadata = server.serializeMetadata();

		assertTrue(testMetadata.equalsIgnoreCase(resMetadata));
	}

	@Test
	public void testKVServerShutdown() {
		KVMessage shutdown = new KVMessage("SERVER_SHUTDOWN");
		server.handleMessage(shutdown);
		assertFalse(server.isRunning());
	}

	@Test
	public void testKVServerStart() {
		KVMessage start = new KVMessage("SERVER_START");
		KVMessage res = server.handleMessage(start);
		assertTrue(res.getStatus() == StatusType.SERVER_START_SUCCESS);
	}

	@Test
	public void testConnection() {
		assertTrue("Connection to KVServer should be established.", kvStore.connected);
	}

	@Test
	public void testMetadataBasedRouting() throws Exception {
		// Assuming metadata update to simulate server responsible for a specific key
		// range
		String simulatedMetadata = "0,7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,localhost:" + serverPort + ";";
		kvStore.updateMetadata(simulatedMetadata);

		// Perform a PUT operation that should succeed based on the updated metadata
		String key = "keyWithinRange"; // Choose a key that hashes within the above range
		String value = "testValue";
		KVMessage response = kvStore.put(key, value);

		assertTrue("PUT operation should be successful.", 
			StatusType.PUT_SUCCESS == response.getStatus() || 
			StatusType.PUT_UPDATE == response.getStatus());

		// Now test GET operation for the same key
		response = kvStore.get(key);
		assertEquals("GET operation should return the correct value.", value, response.getValue());
	}

	@Test
	public void testPutKVWithInvalidInputs() {
		Exception ex = null;

		try {
			server.putKV(null, "someValue");
		} catch (Exception e) {
			ex = e;
		}

		try {
			server.putKV("someKey", null);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex != null); // Server should throw exception for null value.

	}

	@Test
	public void testConcurrentClientConnections() throws InterruptedException {

		Thread client1 = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					KVStore client = new KVStore("localhost", serverPort);
					client.connect();
					server.putKV("concurrentKey1", "value1");
					client.disconnect();
				} catch (Exception e) {
					assertTrue(e != null);
				}
			}
		});

		Thread client2 = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					KVStore client = new KVStore("localhost", serverPort);
					client.connect();
					KVMessage response = client.get("concurrentKey1");
					assertEquals("Client 2 should retrieve the value set by Client 1.", "value1", response.getValue());
					client.disconnect();
				} catch (Exception e) {
					assertTrue(e != null);
				}
			}
		});

		client1.start();
		// Ensure client1 writes before client2 attempts to read
		Thread.sleep(1000);
		client2.start();

		client1.join();
		client2.join();
	}

	@Test
	public void testRebalance() throws Exception {
		int server2Port = 12346;
		KVServer server2 = new KVServer(server2Port, 0, "None");
		server2.start();
		// Set initial metadata indicating server is responsible for all keys
		server.updateMetadata("0,FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,localhost:" + serverPort + ";");
		server.putKV("rebalanceKey", "rebalanceValue");

		// Update metadata to simulate rebalance, removing responsibility for the key
		server.updateMetadata(
				"0,7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,localhost:"
						+ serverPort + ";80000000000000000000000000000000,FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,localhost:"
						+ server2Port + ";");
		server.rebalance();

		assertFalse("Server should not contain key after rebalance.", server.inStorage("rebalanceKey"));
		server2.kill();
	}

	@Test
	public void testGetWithServerNotResponsible() {
		// Manually setting server metadata to simulate server not responsible for any
		// key
		String outsideRangeMetadata = "0,100,localhost:50002;"; // Key ranges that don't cover the server
		server.updateMetadata(outsideRangeMetadata);

		Exception ex = null;

		try {
			server.getKV("keyOutsideRange");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue("Server should throw exception for key it's not responsible for", ex != null);
	}

	// MS3 TESTS

	@Test
	public void testGetNodeOrder() {
		String testMetadata = "0," +
				"7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF," +
				"localhost:1234;" +
				"80000000000000000000000000000000," +
				"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF," +
				"localhost:5432;";
		server.updateMetadata(testMetadata);
		List<Map.Entry<String, BigInteger[]>> nodeOrder = server.getNodeOrder();

		assertEquals(
				"First in the hash ring should be localhost:1234",
				nodeOrder.get(0).getKey(),
				"localhost:1234");
	}

	@Test
	public void testUpdateReplicas() {

		server.clearStorage();

		KVServer server1 = new KVServer(5566, 0, "None");
		server1.start();

		List<Map.Entry<String, BigInteger[]>> nodePositions = new ArrayList();
		nodePositions.add(
				new AbstractMap.SimpleEntry<String, BigInteger[]>("localhost:" + serverPort, null));
		nodePositions.add(
				new AbstractMap.SimpleEntry<String, BigInteger[]>("localhost:5566", null));

		server.updateReplicas(nodePositions);

		assertEquals("Replica 1 port should be 5566", 5566, server.replica1.port);

		server1.close();
	}

	// @Test
	// public void testReadFromReplica() throws Exception {

	// 	server.clearStorage();

	// 	KVServer server1 = new KVServer(5566, 0, "None");
	// 	server1.start();

	// 	List<Map.Entry<String, BigInteger[]>> nodePositions = new ArrayList();
	// 	nodePositions.add(
	// 			new AbstractMap.SimpleEntry<String, BigInteger[]>("localhost:" + serverPort, null));
	// 	nodePositions.add(
	// 			new AbstractMap.SimpleEntry<String, BigInteger[]>("localhost:5566", null));

	// 	server.updateReplicas(nodePositions);

	// 	kvStore.put("hi", "bye");

	// 	new Thread(new Runnable() {
	// 		@Override
	// 		public void run() {
	// 			try {
	// 				KVStore newClient = new KVStore("localhost", 5566);
	// 				newClient.connect();
	// 				assertEquals(
	// 						"Should retrieve kv pair from replica",
	// 						"bye",
	// 						newClient.get("hi").getValue());
	// 				newClient.disconnect();
	// 			} catch (Exception e) {
	// 				System.out.println(e);
	// 			}
	// 		}
	// 	}).start();

	// 	server1.close();
	// 	// Wait for shutdown process
	// 	Thread.sleep(1000);
	// }

	@Test
	public void testUpdateReadMetadata() {
		String testMetadata = "0," +
				"7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF," +
				"localhost:1234;" +
				"80000000000000000000000000000000," +
				"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF," +
				"localhost:5432;";
		String testReadMetadata = "80000000000000000000000000000000," +
				"7fffffffffffffffffffffffffffffff," +
				"localhost:1234;" +
				"0,ffffffffffffffffffffffffffffffff," +
				"localhost:5432;";
		server.updateMetadata(testMetadata);
		String resMetadata = server.serializeReadMetadata();

		assertTrue(testReadMetadata.equalsIgnoreCase(resMetadata));
	}

	@Test
	public void testReplicaSerializeReadMetadata() throws InterruptedException {
		KVServer server1 = new KVServer(6765, 0, "None");
		// hash = 961003672f6299f436c831ca5017bd64
		KVServer server2 = new KVServer(6766, 0, "None");
		// hash = bc1f1cf83e7c545dae46f4d9eb1ec05b

		server1.ecsAddress = "localhost";
		server1.ecsPort = ecsPort;
		server2.ecsAddress = "localhost";
		server2.ecsPort = ecsPort;

		server1.start();
		server2.start();
		// Wait for startup process
		Thread.sleep(1000);

		String testReadMetadata = "961003672f6299f436c831ca5017bd65," +
				"961003672f6299f436c831ca5017bd64," +
				"localhost:6765;" +
				"bc1f1cf83e7c545dae46f4d9eb1ec05c," +
				"bc1f1cf83e7c545dae46f4d9eb1ec05b," +
				"localhost:6766;";

		String readMetadata = server1.serializeReadMetadata();

		assertTrue(testReadMetadata.equalsIgnoreCase(readMetadata));

		server1.close();
		// Wait for shutdown process
		Thread.sleep(1000);

		server2.close();
		// Wait for shutdown process
		Thread.sleep(1000);

	}

	@Test
	public void testAddReplica2Servers() throws InterruptedException {

		KVServer server1 = new KVServer(6691, 0, "None");
		KVServer server2 = new KVServer(6692, 0, "None");

		server1.ecsAddress = "localhost";
		server1.ecsPort = ecsPort;
		server2.ecsAddress = "localhost";
		server2.ecsPort = ecsPort;

		server1.start();
		server2.start();
		// Wait for startup process
		Thread.sleep(1000);

		assertTrue(server1.replica1.port == 6692);
		assertTrue(server2.replica1.port == 6691);

		server1.close();
		// Wait for shutdown process
		Thread.sleep(1000);
		server2.close();
		// Wait for shutdown process
		Thread.sleep(1000);
	}

	@Test
	public void testRemoveReplica2Servers() throws InterruptedException {

		KVServer server1 = new KVServer(6391, 0, "None");
		KVServer server2 = new KVServer(6392, 0, "None");

		server1.ecsAddress = "localhost";
		server1.ecsPort = ecsPort;
		server2.ecsAddress = "localhost";
		server2.ecsPort = ecsPort;

		server1.start();
		server2.start();
		// Wait for startup process
		Thread.sleep(1000);

		server2.close();
		// Wait for shutdown process
		Thread.sleep(1000);

		assertTrue(server1.replica1 == null);

		server1.close();
		// Wait for shutdown process
		Thread.sleep(1000);
	}

	@Test
	public void testAddReplica3Servers() throws InterruptedException {
		KVServer server1 = new KVServer(6750, 0, "None");
		// hash = 2dab258fe90dd18cd270c6432f679a05
		KVServer server2 = new KVServer(6751, 0, "None");
		// hash = 8864b17b66872e3c9e41aede0d8d19b2
		KVServer server3 = new KVServer(6752, 0, "None");
		// hash = 2f47d15cb7c337b2f1d6eeae558a71b6

		server1.ecsAddress = "localhost";
		server1.ecsPort = ecsPort;
		server2.ecsAddress = "localhost";
		server2.ecsPort = ecsPort;
		server3.ecsAddress = "localhost";
		server3.ecsPort = ecsPort;

		server1.start();
		server2.start();
		server3.start();
		// Wait for startup process
		Thread.sleep(1000);

		assertTrue(server1.replica1.port == 6752);
		assertTrue(server1.replica2.port == 6751);
		assertTrue(server2.replica1.port == 6750);
		assertTrue(server2.replica2.port == 6752);
		assertTrue(server3.replica1.port == 6751);
		assertTrue(server3.replica2.port == 6750);

		server3.close();
		// Wait for shutdown process
		Thread.sleep(1000);

		server2.close();
		// Wait for shutdown process
		Thread.sleep(1000);

		server1.close();
		// Wait for shutdown process
		Thread.sleep(1000);

	}

	@Test
	public void testRemoveReplica3Servers() throws InterruptedException {
		KVServer server1 = new KVServer(4560, 0, "None");
		// hash = b2613358df41b0de93540bf395214154
		KVServer server2 = new KVServer(4561, 0, "None");
		// hash = 11320ed27d6dc10d9259774394faec62
		KVServer server3 = new KVServer(4562, 0, "None");
		// hash = 7965bf3dd617a59150a503bb636a0577

		server1.ecsAddress = "localhost";
		server1.ecsPort = ecsPort;
		server2.ecsAddress = "localhost";
		server2.ecsPort = ecsPort;
		server3.ecsAddress = "localhost";
		server3.ecsPort = ecsPort;

		server1.start();
		server2.start();
		server3.start();

		Thread.sleep(1000);

		server3.close();
		// Wait for shutdown process
		Thread.sleep(1000);

		assertTrue(server1.replica1.port == 4561);
		assertTrue(server1.replica2 == null);
		assertTrue(server2.replica1.port == 4560);
		assertTrue(server2.replica2 == null);

		server2.close();
		// Wait for shutdown process
		Thread.sleep(1000);

		assertTrue(server1.replica1 == null);
		assertTrue(server1.replica2 == null);

		server1.close();
		// Wait for shutdown process
		Thread.sleep(1000);

	}

	@Test 
	public void testHeartbeat() throws Exception {

		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					ServerSocket servSocket = new ServerSocket();
					servSocket.bind(new InetSocketAddress(7777));
                    Socket socket = servSocket.accept();
					InputStream input = socket.getInputStream();
					KVMessage res1 = CommProtocol.receiveMessage(input, true);
					KVMessage res2 = CommProtocol.receiveMessage(input, true);
					assertEquals(res2.getStatus(), StatusType.HEARTBEAT);

				} catch (IOException ioe) {
					System.out.println(ioe);
				}
			}
		}).start();

		KVServer server1 = new KVServer(3543, 0, "None");
		server1.ecsAddress = "localhost";
		server1.ecsPort = 7777;
		server1.start();

	}

	// M4 TESTS
	@Test 
	public void testSubscribe() throws Exception {
		KVMessage res = kvStore.subscribe("key1");
		assertEquals(res.getStatus(), StatusType.SUBSCRIBE_SUCCESS);
		assertEquals(res.getKey(), "key1");
	}

	@Test 
	public void testArbitraryClientSubscribe() throws Exception {
		KVMessage res = kvStore.subscribeAnyClient("key1", "localhost", 3000);
		assertEquals(res.getStatus(), StatusType.SUBSCRIBE_SUCCESS);
		assertEquals(res.getKey(), "key1");
		assertEquals(res.getValue(), "localhost:3000");
	}
	@Test 
	public void testUnsubscribe() throws Exception {
		KVMessage res = kvStore.unsubscribe("key1");
		assertEquals(res.getStatus(), StatusType.UNSUBSCRIBE_SUCCESS);
		assertEquals(res.getKey(), "key1");
	}

	@Test 
	public void testArbitraryClientUnsubscribe() throws Exception {
		KVMessage res = kvStore.unsubscribeAnyClient("key1", "localhost", 3000);
		assertEquals(res.getStatus(), StatusType.UNSUBSCRIBE_SUCCESS);
		assertEquals(res.getKey(), "key1");
		assertEquals(res.getValue(), "localhost:3000");
	}

	@Test 
	public void testUpdateSubscribersMap() throws Exception {
		kvStore.subscribeAnyClient("SomeKey", "localhost", 5000);

		assertTrue(server.getSubscribers().containsKey("SomeKey"));
		assertTrue(server.getSubscribers().get("SomeKey").contains("localhost:5000"));
	}

	@Test 
	public void testSubscribeAndUnsubscribeMap() throws Exception {
		kvStore.subscribeAnyClient("SomeKey", "localhost", 5000);
		kvStore.unsubscribeAnyClient("SomeKey", "localhost", 5000);

		assertTrue(!server.getSubscribers().containsKey("SomeKey"));
	}

	@Test
	public void testSubscribersAfterRebalance() throws Exception {
		int server2Port = 12347;
		KVServer server2 = new KVServer(server2Port, 0, "None");
		server2.start();
		// Set initial metadata indicating server is responsible for all keys
		server.updateMetadata("0,FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,localhost:" + serverPort + ";");

		// Subscribe to rebalanceKey and put it into server storage
		kvStore.subscribe("rebalanceKey");
		server.putKV("rebalanceKey", "rebalanceValue");

		// Update metadata to simulate rebalance, removing responsibility for the key
		server.updateMetadata(
				"0,7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,localhost:"
				+ serverPort + ";80000000000000000000000000000000,FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,localhost:"
				+ server2Port + ";");
		server.rebalance();

		assertTrue(server2.getSubscribers().containsKey("rebalanceKey"));
		server2.kill();
	}

	@Test
	public void testSubscribersAfterRebalanceRealMetadata() throws Exception {
		// server localhost:3321
		// hash = c07c82e5e57d5c5736a70f5e48891e61
		KVServer server2 = new KVServer(4561, 0, "None");
		// hash = 11320ed27d6dc10d9259774394faec62

		server2.start(); 

		kvStore.subscribe("key1"); // c2add694bf942dc77b376592d9c862cd
		kvStore.subscribe("key2"); // 78f825aaa0103319aaa1a30bf4fe3ada

		String metadata =
			"c07c82e5e57d5c5736a70f5e48891e61,11320ed27d6dc10d9259774394faec62,localhost:4561;" +
			"11320ed27d6dc10d9259774394faec63,c07c82e5e57d5c5736a70f5e48891e61,localhost:3321;";
		server.updateMetadata(metadata);
		server2.updateMetadata(metadata);

		server.rebalance();

		System.out.println(server.getSubscribers());
		System.out.println(server2.getSubscribers());
		
		assertTrue(server.getSubscribers().containsKey("key2"));
		assertTrue(server2.getSubscribers().containsKey("key1"));

		server2.kill();

	}

	@Test 
	public void testServerNotResponsibleSubscribe() throws Exception {
		KVServer server2 = new KVServer(6768, 0, "None");

		server2.ecsAddress = "localhost";
		server2.ecsPort = ecsPort;

		server2.start(); 

		Thread.sleep(1000);

		String metadata =
			"c07c82e5e57d5c5736a70f5e48891e61,11320ed27d6dc10d9259774394faec62,localhost:6768;" +
			"11320ed27d6dc10d9259774394faec63,c07c82e5e57d5c5736a70f5e48891e61,localhost:3321;";
		server2.updateMetadata(metadata);

		KVStore newClient = new KVStore("localhost", 6768);
		newClient.connect();
		KVMessage res = newClient.sendKVMessage("SUBSCRIBE key2 localhost:4000"); // c2add694bf942dc77b376592d9c862cd
		newClient.disconnect();
		
		assertEquals(res.getStatus(), StatusType.SERVER_NOT_RESPONSIBLE);

	}

	@Test
	public void testRerouteSubscribe() throws Exception {
		KVServer server1 = new KVServer(6773, 0, "None");
		KVServer server2 = new KVServer(6771, 0, "None");

		server1.start(); 
		server2.start(); 

		String metadata =
			"c07c82e5e57d5c5736a70f5e48891e61,11320ed27d6dc10d9259774394faec62,localhost:6771;" +
			"11320ed27d6dc10d9259774394faec63,c07c82e5e57d5c5736a70f5e48891e61,localhost:6773;";
		server1.updateMetadata(metadata);
		server2.updateMetadata(metadata);

		KVStore newClient = new KVStore("localhost", 6771);
		newClient.connect();
		KVMessage res = newClient.subscribe("SUBSCRIBE key2 localhost:4000"); // c2add694bf942dc77b376592d9c862cd
		// Should reroute to localhost:6773
		newClient.disconnect();
		
		assertEquals(res.getStatus(), StatusType.SUBSCRIBE_SUCCESS);

		server1.kill();
		server2.kill();

	}

}

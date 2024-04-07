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

public class KVReplica {

	private static Logger logger = Logger.getRootLogger();

    public String hostname;
    public int port;

    public int replicaNum;
    private Socket socket;
    private OutputStream output;

    public KVReplica(String hostname, int port, int replicaNum) {
        this.hostname = hostname;
		this.port = port;
        this.replicaNum = replicaNum;
    }

    /**
     * Send a KVMessage to a replica server
     * @param key key to send to replica
     * @param val val to send to replica
     */
    public void putToReplica(String key, String val) {
        try {
            CommProtocol.sendMessage(
                new KVMessage(
                    "PUT_FROM_COORDINATOR_" + replicaNum 
                    + " " + key 
                    + " " + val), output);
        } catch (IOException e) {
            logger.error("Error while sending Coordinator Values to Replica Server " + replicaNum, e);
        }
    }

    public void copyMemoryToReplica(Map<String,String> kvs) {
        for (Map.Entry<String, String> kv: kvs.entrySet()) {
            putToReplica(kv.getKey(), kv.getValue());
        }
    }

    public void connect() {
        logger.info("Connecting to replica " + replicaNum + " at " + hostname + ":" + port);
        try {
            socket = new Socket(hostname, port);
            output = socket.getOutputStream();
        } catch (IOException e) {
            logger.error("Error connecting to Replica Server " + replicaNum, e);
        }
    }

    public void disconnect() {
        logger.info("Disconnecting from replica " + replicaNum + " at " + hostname + ":" + port);
        try {
            output.close();
            socket.close();
        } catch (IOException e) {
            logger.error("Error: ", e);
        }
    }
}
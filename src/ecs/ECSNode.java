package ecs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.math.BigInteger;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import shared.messages.CommProtocol;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;
import shared.Hash;

public class ECSNode implements IECSNode {

	private static Logger logger = Logger.getRootLogger();

    private String name;
    private String hostname;
    private int port;
    private String[] hashRange;
    private StatusType status;

    public Socket socket;
    private InputStream input;
    private OutputStream output;

    private boolean connected;
    private boolean started;

    private boolean heartbeat;

    public ECSNode(String hostname, int port, Socket socket) {
        this.hostname = hostname;
        this.port = port;
        this.socket = socket;
        this.hashRange = null;
        this.status = null;

        name = hostname + ":" + port;
        connected = true;
        started = false;
        heartbeat = true;


        try {
            input = socket.getInputStream();
            output = socket.getOutputStream();
        } catch (IOException e) {
            logger.error("Failed to connect to socket");
        }
    }

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName() {
        return this.name;
    }

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost() {
        return this.hostname;
    }

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort() {
        return this.port;
    }

    /**
     * Get the StatusType of the most recent KVMessage received
     * 
     * @return last StatusType received
     */
    public StatusType getNodeStatus() {
        return this.status;
    }

    /**
     * Set StatusType last received
     * 
     * @param status
     */
    public void setNodeStatus(StatusType status) {
        this.status = status;
    }

    /**
     * @return  array of two strings representing the low and high range 
     * of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange() {
        return this.hashRange;
    }

    /**
     * Set method for hash range
     * 
     * @param hashRange hash range to set
     */
    public void setNodeHashRange(String[] hashRange) {
        this.hashRange = hashRange;
    }

    /**
     * Get hash of own name to determine hash ring position
     * 
     * @return BigInteger of hash ring position
     */
    public BigInteger getPosition() {
        return Hash.hash(this.name);
    }

    /**
     * Set if server associated with node is started
     * 
     * @param started boolean to set to
     */
    public void setStarted(boolean started) {
        this.started = started;
    }

    /**
     * Check if server associated with node is started
     * 
     * @return if server is started
     */
    public boolean isStarted() {
        return this.started;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
        try {
            input = socket.getInputStream();
            output = socket.getOutputStream();
        } catch (IOException e) {
            logger.error("Failed to connect to socket");
        }
    }

    public void setHeartbeat(boolean hb) {
        this.heartbeat = hb;
    }

    public boolean getHeartbeat() {
        return this.heartbeat;
    }


    /**
     * Send server stop message
     * 
     * @return response
     */
    public KVMessage serverStop() {
        return sendMessage(new KVMessage("SERVER_STOP"));
    }

    /**
     * Send server shutdown message
     * 
     * @return response
     */
    public KVMessage serverShutdown() {
        return sendMessage(new KVMessage("SERVER_SHUTDOWN"));
    }

    /**
     * Send server start message
     * 
     * @return response
     */
    public KVMessage serverStart() {
        return sendMessage(new KVMessage("SERVER_START"));
    }

    /**
     * Send a rebalance message
     * 
     * @return response
     */
    public KVMessage rebalance() {
        return sendMessage(new KVMessage("REBALANCE"));
    }

    /**
     * Send generic message to server associated with
     * ECSNode
     * 
     * @param msg KVMessage to send
     * @return response
     */
    public KVMessage sendMessage(KVMessage msg) {

        KVMessage res = null;
        
        try {
            CommProtocol.sendMessage(msg, output);
        } catch (IOException e) {
            logger.error("Connection lost!");
            tearDownConnection();
        }
        // try {
        //     res = CommProtocol.receiveMessage(input, true);
        //     this.status = res.getStatus();
        // } catch (IOException e) {
        //     logger.error("Connection lost!");
        //     tearDownConnection();
        // }

        return res;
    }

    /**
	 * Closes input and output streams and closes the
	 * socket.
	 */
	public void tearDownConnection() {
		connected = false;
		logger.info("Tearing down the connection ...");
		if (socket != null) {
			try {
				input.close();
				output.close();
				socket.close();
			} catch (IOException e) {
				logger.error("Error: ", e);
			}
			socket = null;
			logger.info("Connection closed.");
		}
	}
}

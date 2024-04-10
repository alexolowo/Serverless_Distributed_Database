package client;

import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import shared.messages.CommProtocol;
import shared.messages.KVMessage;

public class KVStoreServerSocket implements Runnable {

	private static Logger logger = Logger.getRootLogger();

    private String address;
    private int port;

    private KVStore kvStore;
    private ServerSocket kvSocket;

    private boolean running;

    public KVStoreServerSocket(KVStore kvStore) {
        this.kvStore = kvStore;
    }

    private boolean initializeServerSocket() {
        logger.info("Initializing KVStore Socket...");

        try {
            kvSocket = new ServerSocket(0);
            this.port = kvSocket.getLocalPort();
            this.address = kvSocket.getInetAddress().getHostName();
            kvStore.serverSockPort = this.port;
            kvStore.serverSockAddr = this.address;
            logger.info("KVStore listening on port " + port);
            return true;
        } catch (BindException e) {
            logger.error("Port " + port + " is already bound.");
            return false;
        } catch (Exception e) {
            logger.error("Error: Cannot open server socket. ", e);
            return false;
        }
    }

	/**
	 * Gets whether this thread is running or not.
	 * 
	 * @return true if thread is running.
	 */
	public boolean isRunning() {
		return running;
	}
	
	/**
	 * Sets whether this thread is running or not.
	 * 
	 * @param run boolean to change running to.
	 */
	public void setRunning(boolean run) {
		running = run;
	}


    /**
     * Run loop for KVStoreServerSocket thread
     */
    public void run() {
        setRunning(initializeServerSocket());

        if (kvSocket != null) {
            while (isRunning()) {
                try {
                    Socket socket = kvSocket.accept();

                    logger.info("KVServer connected at "
                        + socket.getInetAddress().getHostName()
                        + ":" + socket.getPort());

                    InputStream input = socket.getInputStream();
                    KVMessage msg = CommProtocol.receiveMessage(input, false);

                } catch (Exception e) {
                    logger.error("Error: Unable to establish connection.\n", e);
                }	
            }
            try {
                kvSocket.close();
            } catch (IOException ioe) {
                logger.error("Error: ", ioe);
            }
            
        }
        logger.info("KVStore ServerSocket closed.");
    }
}
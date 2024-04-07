package app_kvECS;

import java.util.Map;

import app_kvClient.KVClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import ecs.ECSNode;
import shared.messages.CommProtocol;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

public class ECSServerSocket implements Runnable {

	private static Logger logger = Logger.getRootLogger();

    private String address;
    private int port;

    private ECSClient ecs;
    private ServerSocket ecsSocket;

    private boolean running;
    
    public ECSServerSocket(ECSClient ecs, String address, int port) {
        this.ecs = ecs;
        this.address = address;
        this.port = port;
    }

    private boolean initializeServerSocket() {
        logger.info("Initializing ECS Socket...");

        try {
            ecsSocket = new ServerSocket();
            ecsSocket.bind(new InetSocketAddress(address, port));
            logger.info("ECS listening on port " + port);
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
     * Run loop for ECSConnection thread
     */
    public void run() {
        setRunning(initializeServerSocket());

        if (ecsSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = ecsSocket.accept();
                    ECSNodeConnection conn = new ECSNodeConnection(client, ecs);
                    new Thread(conn).start();

                    logger.info("ECS connected to KVServer at "
                        + client.getInetAddress().getHostName()
                        + ":" + client.getPort());
                } catch (Exception e) {
                    logger.error("Error: Unable to establish connection.\n", e);
                }	
            }
            try {
                ecsSocket.close();
            } catch (IOException ioe) {
                logger.error("Error: ", ioe);
            }
            
        }
        logger.info("ECS ServerSocket closed.");
    }
}
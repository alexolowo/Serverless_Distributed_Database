package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import shared.messages.KVMessage;
import shared.messages.CommProtocol;

public class ClientConnection implements Runnable {
    
	private static Logger logger = Logger.getRootLogger();
	
	public boolean isOpen;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;

	private KVServer listener;

    /**
	 * Constructs a new ClientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer listener) {
		this.clientSocket = clientSocket;
		this.listener = listener;
		this.isOpen = true;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			
			while (isOpen) {
				try {
					KVMessage latestMsg = 
						CommProtocol.receiveMessage(input, false);
					// call message handler
					KVMessage res = listener.handleMessage(latestMsg);
					CommProtocol.sendMessage(res, output);
					
				// connection terminated or lost
				} catch (IOException ioe) {
					logger.info("Connected aborted or lost");
					isOpen = false;
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error: Connection could not be established. ", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null) {
					logger.info("Closing client connection...");
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error: Unable to tear down connection. ", ioe);
			}
		}
	}
	
}

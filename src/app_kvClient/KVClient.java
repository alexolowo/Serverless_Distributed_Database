package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import client.KVCommInterface;
import client.KVStore;
import shared.PerformanceEvaluation;
import shared.messages.KVMessage;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

public class KVClient implements IKVClient {

	private static Logger logger = Logger.getRootLogger();

    private KVStore kvStore;

    private boolean stop = false;
	private static final String PROMPT = "> ";
	private BufferedReader stdin;

	/**
     * Creates a new connection to hostname:port
	 * 
	 * @param hostname hostname of server to connect to
	 * @param port port of server to connect to
     * @throws Exception
     *      when a connection to the server can not be established
     */
    @Override
    public void newConnection(String hostname, int port) throws Exception {
        kvStore = new KVStore(hostname, port);
		kvStore.addListener(this);
		kvStore.connect();
    }

    /**
     * Get the current instance of the Store object
     * @return  instance of KVCommInterface
     */
    @Override
    public KVCommInterface getStore() {
        return kvStore;
    }

	/**
	 * Prints new incoming message from KVStore to CLI.
	 * 
	 * @param msg message to print
	 */
    public void handleNewMessage(KVMessage msg) {
		if(!stop) {
			System.out.println("Received " +
				msg.getStatus() +
				" " + msg.getKey() +
				" " + msg.getValue());
			System.out.print(PROMPT);
		}
	}

	/**
	 * Prints new incoming status from KVStore to CLI.
	 * 
	 * @param status current status of the KVStore socket.
	 */
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated: " 
					+ kvStore.address + " / " + kvStore.port);
			
		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: " 
					+ kvStore.address + " / " + kvStore.port);
			System.out.print(PROMPT);
		}
		
	}

	/**
	 * Handles incoming commands from the command
	 * line input and performs the correct action.
	 * 
	 * @param cmdLine string parsed from the command 
	 * 		line input.
	 */
	private void handleCommand(String cmdLine) {
		// only split by first 3 spaces
		String[] tokens = cmdLine.trim().split("\\s+", 3);  

		switch(tokens[0]) {
			case "quit":
				stop = true;
				if (kvStore != null) {
					kvStore.disconnect();
				}
				System.out.println(PROMPT + "Application exit.");
				break;
			case "connect":
				if(tokens.length == 3) {
					try{
						String serverAddress = tokens[1];
						int serverPort = Integer.parseInt(tokens[2]);
						newConnection(serverAddress, serverPort);
					} catch(NumberFormatException nfe) {
						System.out.println(
							"Not a valid address. Port must be a number.");
						logger.info("Unable to parse argument <port>", nfe);
					} catch(UnknownHostException e) {
						System.out.println("Unknown Host!");
						logger.info("Unknown Host.", e);
					} catch(Exception e) {
						System.out.println("Exception:\n" + e);
						logger.warn("Exception.", e);
					}
				} else {
					System.out.println(
						"Invalid number of parameters.\n"
						+ "Usage: connect <hostname> <port>");
				}
				break;
			case "put":
				if (tokens.length == 3) {
					if (kvStore != null && kvStore.connected) {
						String key = tokens[1];
						String value = tokens[2];
						try {
							KVMessage res = kvStore.put(key, value);
						} catch (Exception e) {
							logger.error(
								"Error while putting " 
								+ key + " : " + value
								+ ": " + e);
						}
					} else {
						System.out.println("Not connected to a server.");
					}
				} else {
					System.out.println(
						"Invalid number of parameters. Usage: put <key> <val>");
				}	
				break;
			case "get":
				if (tokens.length == 2) {
					if (kvStore != null && kvStore.connected) {
						String key = tokens[1];
						try {
							KVMessage res = kvStore.get(key);
						} catch (Exception e) {
							logger.error("Unable to get " + key);
						}

					} else {
						System.out.println("Not connected to a server.");
					}
				} else {
					System.out.println(
						"Invalid number of parameters. Usage: get <key>");
				}
				break;
			case "subscribe":
				if (tokens.length == 2) {

				} else {
					System.out.println(
						"Invalid number of parameters. Usage: subscribe <key>");
				}
				break;
			case "unsubscribe":
				if (tokens.length == 2) {

				} else {
					System.out.println(
						"Invalid number of parameters. Usage: subscribe <key>");
				}
				break;
			case "disconnect":
				if (kvStore != null) {
					kvStore.disconnect();
					kvStore = null;
				}
				break;
			case "logLevel":
				if(tokens.length == 2) {
					String level = setLevel(tokens[1]);
					if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
						System.out.println("Not a valid log level.");
						System.out.println(LogSetup.getPossibleLogLevels());
					} else {
						System.out.println(PROMPT + 
								"Log level changed to level " + level);
					}
				} else {
					System.out.println("Invalid number of parameters.");
				}
				break;
			case "help":
				printHelp();
				break;
			default:
				System.out.println("Unknown command");
				printHelp();
				break;
		}
	}

	/**
	 * Prints CLI help.
	 */
	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("CLIENT APPLICATION HELP (Usage):\n");
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("put <key> <val>");
		sb.append("\t sends a put request to the storage server \n");
		sb.append(PROMPT).append("get <key> <val>");
		sb.append("\t sends a get request to the storage server \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t disconnects from the server \n");
		sb.append(PROMPT).append("subscribe <key>");
		sb.append("\t sends a subscribe request to the storage server \n");
		sb.append(PROMPT).append("unsubscribe <key>");
		sb.append("\t sends an unsubscribe request to the storage server \n");
		
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}

	/**
	 * Sets log level for the log4j logger of the client.
	 * 
	 * @param levelString logging level to change to
	 * @return level that the logger was changed to
	 */
	private String setLevel(String levelString) {
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

     /**
     * Main loop for CLI display and handling input.
     */
	private void run_cli() {
        while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				System.out.println(
					"CLI does not respond. Application terminated ");
                System.out.println(e);
			}
		}
    }

     /**
     * Main entry point for the client application. 
     * @param args are unused in the client application.
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.ALL);
			KVClient app = new KVClient();
			app.run_cli();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
    }
}



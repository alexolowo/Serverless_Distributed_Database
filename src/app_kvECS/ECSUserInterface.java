package app_kvECS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;


import ecs.IECSNode;
import ecs.ECSNode;

public class ECSUserInterface extends Thread {

	private static Logger logger = Logger.getRootLogger();

	private static final String PROMPT = "> ";
	private BufferedReader stdin;
    private boolean stop = false;

    private ECSClient ecs;

    public ECSUserInterface(ECSClient ecs) {
        this.ecs = ecs;
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
     * Prints ECS help.
     */
    private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECS APPLICATION HELP (Usage):\n");
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("list");
		sb.append("\t\t lists all nodes\n");
		sb.append(PROMPT).append("add <n>");
		sb.append("\t adds n nodes to the ring. <n>: integer\n");
		// sb.append(PROMPT).append("disconnect");
		// sb.append("\t\t\t disconnects from the server \n");
		
		sb.append(PROMPT).append("logLevel");
		sb.append("\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t exits the program");
		System.out.println(sb.toString());
    }


    /**
	 * Handles incoming commands from the command
	 * line input and performs the correct action.
	 * 
	 * @param cmdLine string parsed from the command 
	 * 		line input.
	 */
	private void handleCommand(String cmdLine) {
		// only split by first 2 spaces
		String[] tokens = cmdLine.trim().split("\\s+", 2);  

		if (tokens[0].equals("quit")) {	
			stop = true;
			System.out.println(PROMPT + "Application exit.");
			System.exit(1);
		
		} else if (tokens[0].equals("list")) {
            System.out.println("NAME / HOST / PORT / KEYRANGE");
            try {
                for (Map.Entry<String, IECSNode> entry : ecs.getNodes().entrySet()) {
                    IECSNode node = entry.getValue();
                    String[] kr = node.getNodeHashRange();
                    System.out.println(
                        entry.getKey() + " / " +
                        node.getNodeHost() + " / " +
                        node.getNodePort() + " / " +
                        kr[0] + "-" + kr[1]);
                }
            } catch (Exception e) {
                System.out.println("Unable to print nodes");
            }

        } else if (tokens[0].equals("remove")) {
			if (tokens.length == 2) {
				ecs.removeNodes(Arrays.asList(Arrays.copyOfRange(tokens, 1, tokens.length)));
			}
		} else if (tokens[0].equals("start")) {
			ecs.start();
		} else if (tokens[0].equals("add")) {
            if (tokens.length == 2) {
				try {
					int n = Integer.parseInt(tokens[1]);
					ecs.addNodes(n, "None", 0);
				} catch (NumberFormatException nfe) {
					System.out.println(
						"Error: Invalid argument <n>! Not an integer!");
				}
			} else {
                System.out.println("Invalid number of parameters.");
			}
        } else if (tokens[0].equals("logLevel")) {
            if (tokens.length == 2) {
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
        } else if(tokens[0].equals("help")) {
		 	printHelp();
		} else {
			System.out.println("Unknown command");
		}
	}

    /**
     * Main loop for CLI display and handling input.
     */
	public void run() {
        while(!stop) {
			stdin = new BufferedReader(
                new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				System.out.println(
					"CLI not responding. Application terminated ");
                System.out.println(e);
			}
		}
    }
}

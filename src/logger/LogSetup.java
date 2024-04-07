package logger;

import java.io.IOException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Represents the initialization for the server logging with Log4J.
 */
public class LogSetup {

	public static final String UNKNOWN_LEVEL = "UnknownLevel";
	private static Logger logger = Logger.getRootLogger();
	private String logdir;
	
	/**
	 * Constructs the logger for the server and client. 
	 * Logs can be appended to the console output and are written
	 * into a specified logfile.
	 * 
	 * @param logdir the destination (i.e. directory + filename) for the 
	 * 		persistent logging information.
	 * @param level the level of logging detail that the logger records.
	 * @throws IOException if the log destination could not be found.
	 */
	public LogSetup(String logdir, Level level) throws IOException {
		this.logdir = logdir;
		initialize(level);
	}

	/**
	 * Initializes the logger by opening the logfile and setting the
	 * logging level.
	 * 
	 * @param level the level of logging detail that the logger records.
	 * @throws IOException if the log destination could not be found.
	 */
	private void initialize(Level level) throws IOException {
		PatternLayout layout = new PatternLayout( "%d{ISO8601} %-5p [%t] %c: %m%n>" );
		FileAppender fileAppender = new FileAppender( layout, logdir, true );		
	    
	    ConsoleAppender consoleAppender = new ConsoleAppender(layout);
		logger.addAppender(consoleAppender);
		logger.addAppender(fileAppender);
		logger.setLevel(level);
	}
	
	/**
	 * Determines if a provided string represents a valid log4j
	 * logging level.
	 * 
	 * @param levelString the string to test.
	 * @return a boolean that is true if the log level is valid.
	 */
	public static boolean isValidLevel(String levelString) {
		boolean valid = false;
		
		if(levelString.equals(Level.ALL.toString())) {
			valid = true;
		} else if(levelString.equals(Level.DEBUG.toString())) {
			valid = true;
		} else if(levelString.equals(Level.INFO.toString())) {
			valid = true;
		} else if(levelString.equals(Level.WARN.toString())) {
			valid = true;
		} else if(levelString.equals(Level.ERROR.toString())) {
			valid = true;
		} else if(levelString.equals(Level.FATAL.toString())) {
			valid = true;
		} else if(levelString.equals(Level.OFF.toString())) {
			valid = true;
		}
		
		return valid;
	}
	
	/**
	 * Gets a string representing all possible log levels for
	 * the client/server CLIs.
	 * 
	 * @return a string representing valid log4j levels.
	 */
	public static String getPossibleLogLevels() {
		return "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF";
	}
}

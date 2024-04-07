package shared.messages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class CommProtocol {

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    
	private static Logger logger = Logger.getRootLogger();

    /**
	 * Method sends a KVMessage to an output stream. Writes the
	 * bytes representation of the message to the output stream.
	 * @param msg the message that is to be sent.
	 * 
	 * @param output the output stream to write the msg bytes to.
	 * @throws IOException I/O error that occurred when writing to 
	 * 		output stream.
	 */
	public static void sendMessage(KVMessage msg, OutputStream output) 
            throws IOException {
		byte[] msgBytes = msg.msgBytes;
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Sent a message:" +
            "\nStatus: " + msg.getStatus() +
			"\nKey: " + msg.getKey() + 
			"\nVal: " + msg.getValue());
    }

	/**
	 * Method receives a KVMessage from an input stream. Reads in
	 * bytes from the input stream into a byte buffer until the
	 * message delimiter is reached or until DROP_SIZE is reached.
	 * Constructs a new KVMessage from the msg bytes.
	 * 
	 * @param input the input stream to read msg bytes from.
	 * @param isClient if the method is being used by client
	 * @return a KVMessage constructed from bytes read.
	 * @throws IOException some I/O error that occured when
	 * 		reading from input stream.
	 */
	public static KVMessage receiveMessage(InputStream input, boolean isClient) 
            throws IOException {		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;

		long startTime = System.nanoTime();
		long elapsed = 0;
		
		while(read != 13 && reading && elapsed < 2) {/* carriage return */
			if (isClient) {
				elapsed = TimeUnit.SECONDS.convert(
					System.nanoTime() - startTime, 
					TimeUnit.NANOSECONDS);
			}
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and numbers */
			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			
			/* stop reading if DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}

		if (elapsed >= 2) {
			throw new IOException("Unable to read from input stream.");
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		/* build final KVMessage */
		KVMessage msg = new KVMessage(msgBytes);
		logger.info("Received a message:" +
			"\nStatus: " + msg.getStatus() +
			"\nKey: " + msg.getKey() + 
			"\nVal: " + msg.getValue());
		return msg;
    }
}

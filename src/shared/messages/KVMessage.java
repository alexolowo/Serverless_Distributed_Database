package shared.messages;

import java.io.IOException;

import org.apache.log4j.Logger;

public class KVMessage implements IKVMessage {

	public String msg;
	public byte[] msgBytes;

	private StatusType status;
	private String key;
	private String value;

	// defines the delimiter
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;

	private static Logger logger = Logger.getRootLogger();

	/**
     * Constructs a Message object with an input byte array.
     * 
     * @param bytes the bytes that form the message in ASCII coding.
     */
	public KVMessage(byte[] bytes) {
		this.msgBytes = addCtrChars(bytes);
		this.msg = new String(bytes);
		splitMessage();
	}

	/**
     * Constructs a Message object with an input String.
     * 
     * @param msg the String that forms the message.
     */
	public KVMessage(String msg) {
		this.msg = msg;
		this.msgBytes = toByteArray(msg);
		splitMessage();
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	@Override
	public String getKey() {
		return this.key;
	}
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	@Override
	public String getValue() {
		return this.value;
	}
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	@Override
	public StatusType getStatus() {
		return this.status;
	}
	
	/**
	 * Implementation of addCtrChars from m0 code to add a delimiter
	 * to the bytes of a message.
	 * 
	 * @return adds delimiter to the beginning of a byte array
	 * 
	 * @param bytes the bytes to add control characters to
	 */
	private byte[] addCtrChars(byte[] bytes) {
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}

	/**
	 * @return byte array formed from the string value of s.
	 * 
	 * @param s the string to turn into a byte[]
	 */
	private byte[] toByteArray(String s){
		byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}

	/**
	 * Takes a msg in text format at splits
	 * @param msg input message to be split into status, key, and value
	 */
	private void splitMessage() {
		// trim out excess whitespace, then split by first 2 spaces
		String[] splitMsg = this.msg.trim().split("\\s+", 3);

		try {
			this.status = StatusType.valueOf(splitMsg[0].toUpperCase());
		} catch (IllegalArgumentException e) {
			logger.info("Not a valid status type: " + splitMsg[0]);
			logger.info("Treating full msg as value");
			this.status = null;
			this.key = null;
			this.value = this.msg.trim();
			return;
		}

		this.key = splitMsg.length >= 2 ? splitMsg[1]: null;
		this.value = splitMsg.length == 3 ? splitMsg[2]: null;
	}

	
}


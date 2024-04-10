package shared.messages;

import java.lang.module.ModuleDescriptor;

public interface IKVMessage {
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_FROM_COORDINATOR_1, /* Put - request from coordinator */
		PUT_FROM_COORDINATOR_2, /* Put - request from coordinator */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */

		KEYRANGE, /* Keyrange - request (from client to server) */
		KEYRANGE_READ,
		KEYRANGE_UPDATE,
		KEYRANGE_SUCCESS, /* Keyrange - request succesful, return keyrange */

		REBALANCE, /* Rebalance - request (ECS to server) */
		REBALANCE_SUCCESS, /* Rebalance - request successful, keys rebalanced */
		REBALANCE_ERROR, /* Rebalance - request not successful */

		SERVER_STOPPED, /* Server is stopped, no requests are processed */
		SERVER_WRITE_LOCK, /* Server locked for write, only get possible */
		SERVER_NOT_RESPONSIBLE, /* Request not successful, server not responsible for key */

		NEW_SERVER, /* Notify ECS of server */
		KILLING_MYSELF, /* Notify ECS of server self-shutdown */
		SERVER_START, /* ECS signal to start accepting requests */
		SERVER_START_SUCCESS, /* Server notify ECS of start success */
		SERVER_STOP, /* ECS signal to stop server accepting requests */
		SERVER_STOP_SUCCESS, /* Server notify ECS of stop success */
		SERVER_SHUTDOWN, /* ECS signal to shutdown server */
		SERVER_SHUTDOWN_SUCCESS, /* Server notify ECS of shutdown success */
		SERVER_SHUTDOWN_ERROR, /* Server notify ECS of shutdown failure */
		HEARTBEAT, /* Notify ECS of server heartbeat */

		SUBSCRIBE, /* Notify KVServer of a client subscribe to key */
		UNSUBSCRIBE /* Notify KVServer of client unsubscribe to key */
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();
	
}



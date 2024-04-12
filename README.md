# How to run

1. Start up an ECS server.

`java -jar m4-ecs.jar -p 3000`

2. Start up 1+ KVServers. Make sure they each have their own unique storage directory.

`java -jar m4-server.jar -p <port> -b localhost:3000 -d <dir>`

3. Start up 1+ KVClients. Connect to a KVServer and put in keys. Subscribe to keys if you want to be informed of changes.

# Milestone 4 (subscription and notification mechanism) list of changes
- API Extension for subscribe and unsubscribe operation
    - KVMessage
        - Add "SUBSCRIBE key hostname:port" to KVMessage
            - Note that the subscribe message must contain the address of the subscribed client, because this will be stored and passed between KVServers (so the SUBSCRIBE message might not be coming from the interested client itself).
        - Add "UNSUBSCRIBE key hostname:port" to KVMessage
        - Add "SUBSCRIBE_SUCCESS" to KVMessage
        - Add "UNSUBSCRIBE_SUCCESS" to KVMessage
    - KVStore:
        - Give KVStore the ability to accept incoming connections from KVServers with its own socket and port running on a new thread, so it can receive PUT_SUCCESS, PUT_UPDATE, or DELETE_SUCCESS messages related to keys it is subscribed to.
        - Give KVStore the ability to send SUBSCRIBE and UNSUBSCRIBE messages. Create new methods to support this and add them as a command on the UI.
    - KVServer:
        - Create a new HashMap called "subscribers". The keys are all the same as in "kvs", but the value is a comma-separated string containing the addresses of subscribed clients (e.g. "localhost:3001,localhost:4000")
        - Handling for SUBSCRIBE/UNSUBSCRIBE. If server not responsible for key, send SERVER_NOT_RESPONSIBLE. Key doesn't have to exist to be subscribed to, but must be in the write keyrange of the server.
        - Whenever a put request from one KVServer to another is performed during the rebalance method, it checks if there are subscribers associated with the key and for each of them sends a "SUBSCRIBE key addr" 
- Update subscribers with all data mutations
    - KVStore:
        - Add handling of PUT_SUCCESS, PUT_UPDATE, or DELETE_SUCCESS messages: it just prints on the CLI.
    - KVServer:
        - When responding to a PUT request, send a copy of res to all subscribers of the key.

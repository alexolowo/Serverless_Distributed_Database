# How to run

1. Start up an ECS server.

`java -jar m2-ecs.jar -p 3000`

2. Start up 1+ KVServers. Make sure they each have their own unique storage directory.

`java -jar m2-server.jar -p <port> -b localhost:3000 -d <dir>`

3. Start up 1+ KVClients. Connect to a KVServer and put in keys.

# Todo: Milestone 4 (notification mechanism)
- API Extension for subscribe and unsubscribe operation
    - KVMessage: (DONE)
        - Add "SUBSCRIBE key hostname:port" to KVMessage
            - Note that the subscribe message must contain the address of the subscribed client, because this will be stored and passed between KVServers (so the SUBSCRIBE message might not be coming from the interested client itself).
        - Add "UNSUBSCRIBE key hostname:port" to KVMessage
    - KVStore: (DONE)
        - Give KVStore the ability to accept incoming connections from KVServers with its own socket and port running on a new thread, so it can receive PUT_UPDATE or DELETE_SUCCESS
        - Give KVStore the ability to send SUBSCRIBE and UNSUBSCRIBE messages. Create two methods for this and add them as a command on the UI.
    - KVServer:
        - Create a new HashMap called "subscribers". The keys are all the same as in "kvs", but the values are a set of strings containing the addresses of subscribed clients (e.g. "localhost:3001")
        - Handling for SUBSCRIBE/UNSUBSCRIBE. If server not responsible for key, send SERVER_NOT_RESPONSIBLE. Key doesn't have to exist to be subscribed to.
        - Whenever a put request from one KVServer to another is performed, it checks if there are subscribers associated with the key and for each of them sends a "SUBSCRIBE key addr" 
- Update subscribers with all data mutations
    - KVStore:
        - Add handling of PUT_UPDATE and DELETE_SUCCESS messages; it might simply print.
    - KVServer:
        - When responding with a PUT_UPDATE or DELETE_SUCCESS message, send a copy to all subscribers associated with the key.
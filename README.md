# How to run

1. Start up an ECS server.

`java -jar m2-ecs.jar -p 3000`

2. Start up 1+ KVServers. Make sure they each have their own unique storage directory.

`java -jar m2-server.jar -p <port> -b localhost:3000 -d <dir>`

3. Start up 1+ KVClients. Connect to a KVServer and put in keys.

# Milestone 3 info

- Create replicas: (Alex)
    - 2 closest successors are designated as replicas. KVServer can parse and keep track of who its successors are from metadata and designate them as replicas.
    - each server keeps replicas of its 2 closest predecessors. KVServer can parse who its predecessors are from metadata.
    - Figure out how to separate replicated data vs coordinator's data
        - 3 kv maps: kvs (coordinator's data), kvs_replica1, kvs_replica2
        - in storage: modify json format {kvs: [{}], kvs_replica1: [{}], kvs_replica2: [{}]}, or different .json files (easier)
    - Whenever a PUT request is received, PUT to both successors.
        - create a new request type to put from coordinator to replica: PUT_FROM_COORDINATOR_1, PUT_FROM_COORDINATOR_2
        - use the socket to figure out where the request is coming from and put in the appropriate storage
    - Implement KEYRANGE_READ message and handling. Maintain a 2nd set of metadata that includes replica extended keyranges (its own keyrange, its predecessor's keyrange, its 2nd predecessor's keyrange)
    - If a replica is being read and it has the key, make sure key is delivered instead of insisting SERVER_NOT_RESPONSIBLE

- Failure detection: (Petra)
    - Use heartbeat to detect KVServer liveness: (DONE)
        - Send heartbeat to ECS every X seconds using a thread
        - Need to change ECS architecture to accept (DONE)
        - ECS is waiting for these and processes these commands (DONE)
        - Implement the actual check in its own thread.
        - One-way to reduce network traffic.
        - When a KVServer fails remove it from the ring (TODO)
    - Then invoke a special "failure rebalance": TODO after replicas
        - Draw out which nodes are affected
        - Should be able to handle more than one failed node at the same time, but what happens if failure occurs during recovery process?
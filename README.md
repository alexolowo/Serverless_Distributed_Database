# Distributed Systems Course Project: Serverless Distributed Database

This project is a serverless distributed database developed as part of the Distributed Systems course. The project involved collaboration between two team members, and it showcases various features and mechanisms necessary for a robust and scalable distributed database. This README provides an overview of the project, its milestones, and instructions on how to run it.

## Table of Contents

1. [Project Overview](#project-overview)
2. [Milestones](#milestones)
   - [Milestone 4: Subscription and Notification Mechanism](#milestone-4-subscription-and-notification-mechanism)
   - [Milestone 3: Replication and Failure Detection](#milestone-3-replication-and-failure-detection)
3. [How to Run](#how-to-run)
   - [Start ECS Server](#start-ecs-server)
   - [Start KVServers](#start-kvservers)
   - [Start KVClients](#start-kvclients)

## Project Overview

This serverless distributed database is designed to be fault-tolerant and scalable, with features such as data replication, subscription-based notifications, and failure detection mechanisms. The project implements a key-value store distributed across multiple servers, ensuring data availability and consistency.

## Milestones

### Milestone 4: Subscription and Notification Mechanism

**List of Changes:**

1. **API Extension for Subscribe and Unsubscribe Operations:**
   - Added "SUBSCRIBE key hostname:port" to `KVMessage`.
   - Added "UNSUBSCRIBE key hostname:port" to `KVMessage`.
   - Added "SUBSCRIBE_SUCCESS" and "UNSUBSCRIBE_SUCCESS" to `KVMessage`.

2. **KVStore Enhancements:**
   - Ability to accept incoming connections from KVServers on a new thread to receive PUT_SUCCESS, PUT_UPDATE, or DELETE_SUCCESS messages for subscribed keys.
   - Methods to send SUBSCRIBE and UNSUBSCRIBE messages, with corresponding UI commands.

3. **KVServer Enhancements:**
   - Introduced a `HashMap` called "subscribers" to track subscribed clients for each key.
   - Handling of SUBSCRIBE/UNSUBSCRIBE requests, including forwarding to the responsible server if necessary.
   - Notification of subscribed clients on data mutations (PUT requests).

4. **Notification Handling in KVStore:**
   - Print messages for PUT_SUCCESS, PUT_UPDATE, or DELETE_SUCCESS events related to subscribed keys.

### Milestone 3: Replication and Failure Detection

**Replication:**

1. **Replica Management:**
   - Designated 2 closest successors as replicas for each KVServer.
   - Each server maintains replicas of its 2 closest predecessors.
   - Separated coordinator's data from replicated data using three key-value maps: `kvs`, `kvs_replica1`, and `kvs_replica2`.
   - Modified storage format to handle different types of data.

2. **Replication Process:**
   - PUT requests to successors during normal operations.
   - Introduced new request types: `PUT_FROM_COORDINATOR_1` and `PUT_FROM_COORDINATOR_2`.

3. **Read Handling:**
   - Implemented `KEYRANGE_READ` to handle replica keyranges.
   - Delivered keys from replicas if available, instead of returning SERVER_NOT_RESPONSIBLE.

**Failure Detection:**

1. **Heartbeat Mechanism:**
   - Implemented heartbeat messages to detect KVServer liveness.
   - ECS server processes these heartbeats and checks liveness in its own thread.

2. **Failure Handling:**
   - Planned mechanism to remove failed nodes from the ring and perform a "failure rebalance".
   - Ability to handle multiple node failures simultaneously and during recovery processes.

## How to Run

### Start ECS Server

To start the ECS (Elastic Container Service) server, use the following command:

```bash
java -jar m4-ecs.jar -p 3000
```

### Start KVServers

Start one or more KVServers. Each KVServer must have a unique storage directory:

```bash
java -jar m4-server.jar -p <port> -b localhost:3000 -d <dir>
```

Replace `<port>` with the desired port number and `<dir>` with the unique storage directory for each server.

### Start KVClients

Start one or more KVClients to connect to a KVServer and interact with the key-value store. KVClients can also subscribe to keys to receive notifications on changes:

```bash
java -jar m4-client.jar -p <port> -b <kvserver-address:port>
```

Use the appropriate `<port>` and `<kvserver-address:port>` to connect to a running KVServer.

## Conclusion

This README provides an overview of the distributed database project, its milestones, and instructions for running the components. The project demonstrates key concepts in distributed systems, including data replication, subscription-based notifications, and failure detection, showcasing a robust and scalable solution.

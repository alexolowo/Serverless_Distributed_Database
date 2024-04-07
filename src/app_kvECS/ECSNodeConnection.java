package app_kvECS;

import java.io.InputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import org.apache.log4j.Logger;

import shared.messages.KVMessage;
import shared.messages.CommProtocol;
import ecs.IECSNode;

public class ECSNodeConnection implements Runnable {

    private static Logger logger = Logger.getRootLogger();

    private Socket socket;
    private InputStream input;

    private ECSClient ecs;

    public boolean isOpen;

    private String serverAddr;
    private int serverPort;

    public ECSNodeConnection(Socket socket, ECSClient ecs) {
        this.socket = socket;
        this.ecs = ecs;
        this.isOpen = true;
    }

    private void handleMsg(KVMessage msg) {
        IECSNode node;

        switch (msg.getStatus()) {
            case NEW_SERVER:
                serverAddr = msg.getKey();
                serverPort = Integer.parseInt(msg.getValue());

                // make this threaded/call back to main thread
                ecs.addServerToPool(serverAddr, serverPort, socket);

                logger.info(
                        "New server connected to ECS: " + serverAddr +
                                ":" + serverPort);
                break;
            case KILLING_MYSELF:
                serverAddr = msg.getKey();
                serverPort = Integer.parseInt(msg.getValue());
                node = ecs.getNodes().get(serverAddr + ":" + serverPort);
                // TODO concurrency errors?
                if (node != null) {
                    // node.setSocket(socket);
                    ecs.removeNodes(
                            Arrays.asList(new String[] { serverAddr + ":" + serverPort }));
                }
                break;
            case HEARTBEAT:
                serverAddr = msg.getKey();
                serverPort = Integer.parseInt(msg.getValue());
                node = ecs.getNodes().get(serverAddr + ":" + serverPort);
                if (node != null) {
                    node.setHeartbeat(true);
                }
                break;
            case REBALANCE_SUCCESS:
                node = ecs.getNodes().get(serverAddr + ":" + serverPort);
                if (node != null) {
                    node.setNodeStatus(msg.getStatus());
                }
                break;
            default:
                break;
        }
    }

    public void run() {
        try {
            input = socket.getInputStream();

            while (isOpen) {
                try {
                    KVMessage msg = CommProtocol.receiveMessage(input, false);
                    handleMsg(msg);
                } catch (IOException e) {
                    logger.error("Error: connection lost");
                    isOpen = false;
                }
            }

        } catch (IOException ioe) {
            logger.error("Error: Connection could not be established. ", ioe);

        } finally {
            try {
                if (socket != null) {
                    logger.info("Closing ECS Node connection...");
                    input.close();
                    socket.close();
                }
            } catch (IOException ioe) {
                logger.error("Error: Unable to tear down connection. ", ioe);
            }
        }

    }
}
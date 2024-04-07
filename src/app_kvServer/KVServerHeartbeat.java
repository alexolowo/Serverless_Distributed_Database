package app_kvServer;

import java.io.OutputStream;
import java.io.IOException;
import java.net.Socket;
import org.apache.log4j.Logger;

import shared.messages.KVMessage;
import shared.messages.CommProtocol;

public class KVServerHeartbeat implements Runnable {

	private static Logger logger = Logger.getRootLogger();

    private int period = 10;

    private Socket ecsSocket;
    private String addr;
    private int port;

    public KVServerHeartbeat(Socket ecsSocket, String addr, int port) {
        this.ecsSocket = ecsSocket;
        this.addr = addr;
        this.port = port;
    }

    public void sendHeartbeat() {
        try {
            OutputStream output = ecsSocket.getOutputStream();
            CommProtocol.sendMessage(
                new KVMessage("HEARTBEAT " + addr + " " + port), output);
        } catch (IOException e) {
            logger.error(e);
        }
    }

    public void run() {
        while (true) {
            try {
                sendHeartbeat();
                Thread.sleep(10000);
            } catch (Exception e) {
                logger.error(e);
            }
        }
    }
}

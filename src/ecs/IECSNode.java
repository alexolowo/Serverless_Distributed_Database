package ecs;

import java.math.BigInteger;
import java.net.Socket;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

public interface IECSNode {

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName();

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost();

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort();

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange();

    public void setSocket(Socket socket);

    public void setHeartbeat(boolean hb);

    public boolean getHeartbeat();

    public StatusType getNodeStatus();

    public void setNodeStatus(StatusType status);

    public BigInteger getPosition();

    public void setStarted(boolean started);

    public boolean isStarted();

    public void setNodeHashRange(String[] hashRange);

    public KVMessage rebalance();

    public KVMessage serverStart();

    public KVMessage serverShutdown();

    public KVMessage serverStop();

    public KVMessage sendMessage(KVMessage msg);

    public void tearDownConnection();


}

package app_kvECS;

import java.util.Set;
import java.util.HashSet;
import org.apache.log4j.Logger;

import ecs.IECSNode;

public class ECSHeartbeatHandler implements Runnable {

	private static Logger logger = Logger.getRootLogger();

    private ECSClient ecs;

    public ECSHeartbeatHandler(ECSClient ecs) {
        this.ecs = ecs;
    }

    private void handleHeartbeats() {
        logger.info("Checking heartbeats");
        Set<String> failNodes = ecs.checkFailed();
        if (failNodes.isEmpty()) {
            logger.info("No node failures");
            return;
        } else {
            ecs.removeNodes(failNodes);
        }
    }

    public void run() {
        while (true) {
            try {
                handleHeartbeats();
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                logger.error("ECS Heartbeat thread interrupted ");
            }
        }
    }
}

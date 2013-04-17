package azkaban.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

/**
 * ExecutorDiscoveryService implementation that uses ZooKeeper (zookeeper.apache.org) as a back-end.
 * <p/>
 * Executors are getting registered by creating an ephemeral nodes. In case executor goes down,
 * zookeeper automatically deletes corresponding node.
 */
public class ExecutorDiscoveryServiceZKImpl implements Watcher, ExecutorDiscoveryService {

    private final static int SESSION_TIMEOUT = 1000;

    // keep trying for 3 seconds
    private final static int CONNECTION_TIMEOUT = 3;

    private static Logger log = Logger.getLogger(ExecutorDiscoveryServiceZKImpl.class);

    private String zkRootNode = null;

    private ZooKeeper zk = null;

    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public ExecutorDiscoveryServiceZKImpl(String connectString,
                                          String zkRootNode) throws IOException {
        super();
        this.zkRootNode = zkRootNode;

        this.zk = new ZooKeeper(connectString, SESSION_TIMEOUT, this);

        try {
            connectedSignal.await(CONNECTION_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new IOException("Could not connect to ZooKeeper, connectionString = " + connectString);
        }

        if (zk.getState() != ZooKeeper.States.CONNECTED) {
            try {
                zk.close();
                zk = null;
            } catch (InterruptedException e) { }
            throw new IOException("Could not connect to ZooKeeper, connectionString = " + connectString);
        }
    }

    @Override
    public List<ExecutorConfig> getActiveExecutors() throws IOException {

        List<ExecutorConfig> executors = new ArrayList<ExecutorConfig>();

        try {

            List<String> children = this.zk.getChildren(this.zkRootNode, false);

            for (String c : children) {
                if (c.startsWith("executor")) {

                    Stat stat = new Stat();

                    log.debug("Getting data, zNode = " + this.zkRootNode + "/" + c);

                    byte[] data = zk.getData(this.zkRootNode + "/" + c, false, stat);

                    log.debug("Got data = " + new String(data));

                    executors.add(ExecutorConfig.fromString(new String(data)));
                }

            }

        } catch (KeeperException e) {
            throw new IOException("Failed getting children of the node = " + this.zkRootNode, e);
        } catch (InterruptedException e) {
            throw new IOException("Failed getting children of the node = " + this.zkRootNode, e);
        }

        return executors;
    }

    @Override
    public void registerExecutor(ExecutorConfig executorConfig) throws IOException {

        log.info("Registering executor: " + executorConfig);

        if (null == zk) {
            throw new IllegalStateException("Failed registering executor. Not connected to ZooKeeper");
        }

        try {

            String rootExecutorNode = this.zkRootNode + "/executor";

            createRootZNode(rootExecutorNode);

            String createdZNodePath = zk.create(rootExecutorNode, executorConfig.toString().getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            log.debug("Created ZNode, path = " + createdZNodePath);

        } catch (KeeperException e) {
            throw new IOException("Failed creating node", e);
        } catch (InterruptedException e) {
            throw new IOException("Failed creating node", e);
        }

    }

    @Override
    public void process(WatchedEvent event) {

        String fmt = "ZooKeeper Watched Event received. Type:%s, State:%s";
        String msg = String.format(fmt, event.getType(), event.getState());

        log.info(msg);

        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    // The client is in the connected state
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        connectedSignal.countDown();
                    }
                    break;
                case Disconnected:
                    // The client is in the disconnected state
                case Expired:
                    //  The serving cluster has expired this session.
                    break;

            }
        }

    }

    /**
     * Method recursively create a zNode path
     */
    private void createRootZNode(String zkRootNodePath) throws KeeperException,
            InterruptedException {

        String[] tokens = zkRootNodePath.split("/");

        String path = "";

        for (int i = 0; i < tokens.length - 1; i++) {
            if (tokens[i].length() != 0) {
                path = path + "/" + tokens[i];
                if (null == zk.exists(path, false)) {
                    zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }
            }
        }

    }

}

package azkaban.executor;

import java.io.IOException;
import java.util.List;

/**
 * Registry of active executor nodes.
 */
public interface ExecutorDiscoveryService {

    /**
     * Every executor should call this method to become discoverable by Web UI
     */
    public List<ExecutorConfig> getActiveExecutors() throws IOException;

    /**
     * Method used by Web UI get get a list of active executors
     */
    public void registerExecutor(ExecutorConfig executorConfig) throws IOException;

    /**
     * Logic of deciding which executor to use sits here
     */
    public ExecutorConfig getExecutor() throws IOException;

}

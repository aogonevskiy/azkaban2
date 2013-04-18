package azkaban.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple collection based executor discovery service implementation
 */
public class ExecutorDiscoveryServicePropsImpl implements ExecutorDiscoveryService {

    private List<ExecutorConfig> executors = new ArrayList<ExecutorConfig>();

    @Override
    public List<ExecutorConfig> getActiveExecutors() throws IOException {
        return executors;
    }

    @Override
    public void registerExecutor(ExecutorConfig executorConfig) throws IOException {
        executors.add(executorConfig);
    }

    @Override
    public ExecutorConfig getExecutor() throws IOException {

        if (0 == executors.size()) {
            throw new IllegalStateException("Executor discovery service does not have any executors registered");
        }

        return executors.get(0);

    }

}

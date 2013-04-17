package azkaban.executor;

import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static junit.framework.Assert.assertEquals;

public class ExecutorDiscoveryServiceZKImplTest {

    @Ignore("need a running instance of zookeeper to run this test")
    @Test
    public void testGetActiveExecutors() throws Exception {

        ExecutorDiscoveryService s = new ExecutorDiscoveryServiceZKImpl("localhost", "/azkaban2_test");

        s.registerExecutor(new ExecutorConfig("host1", 12345));
        s.registerExecutor(new ExecutorConfig("host2", 12345));
        s.registerExecutor(new ExecutorConfig("host3", 12345));

        List<ExecutorConfig> configs = s.getActiveExecutors();

        assertEquals(configs.size(), 3);

    }

}

package azkaban.executor;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ExecutorDiscoveryServiceZKImplTest {

    @Ignore("need a running instance of zookeeper to run this test")
    @Test
    public void testGetActiveExecutorsZK() throws Exception {

        ExecutorDiscoveryService s = new ExecutorDiscoveryServiceZKImpl("localhost", "/azkaban2_test");

        s.registerExecutor(new ExecutorConfig("host1", 12345));
        s.registerExecutor(new ExecutorConfig("host2", 12345));
        s.registerExecutor(new ExecutorConfig("host3", 12345));

        List<ExecutorConfig> configs = s.getActiveExecutors();

        assertEquals(configs.size(), 3);

    }

    @Test
    public void testGetActiveExecutors() throws Exception {

        ZooKeeper zk = mock(ZooKeeper.class);

        ExecutorDiscoveryService s = new ExecutorDiscoveryServiceZKImpl(zk, "/test/test2");

        s.registerExecutor(new ExecutorConfig("host1", 12345));

        // sequence in which zNode gets created:
        verify(zk).create("/test", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        verify(zk).create("/test/test2", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        verify(zk).create("/test/test2/executor", "host1|12345".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

    }


    @Test
    public void testGetExecutor() throws IOException {

        ZooKeeper zk = null;
        ExecutorDiscoveryService service = new ExecutorDiscoveryServiceZKImpl(zk, null);
        ExecutorDiscoveryService spy = spy(service);

        List<ExecutorConfig> configs = new ArrayList<ExecutorConfig>();
        configs.add(new ExecutorConfig("A", 123));

        // just one executor
        doReturn(configs).when(spy).getActiveExecutors();

        ExecutorConfig c = spy.getExecutor();
        assertEquals(c.getHost(), "A");
        assertEquals(c.getPort(), (Integer) 123);

        // adding more executors
        configs.add(new ExecutorConfig("B", 123));
        configs.add(new ExecutorConfig("C", 123));

        // calling method few times
        c = spy.getExecutor();
        assertNotNull(c);
        System.out.println("Random executor = " + c);

        c = spy.getExecutor();
        assertNotNull(c);
        System.out.println("Random executor = " + c);

        c = spy.getExecutor();
        assertNotNull(c);
        System.out.println("Random executor = " + c);

    }

}

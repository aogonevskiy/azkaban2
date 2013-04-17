package azkaban.executor;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class ExecutorConfigTest {

    @Test
    public void testFromString() throws Exception {
        ExecutorConfig config = ExecutorConfig.fromString("localhost|12345");
        assertEquals(config.getHost(), "localhost");
        assertEquals(config.getPort(), Integer.valueOf(12345));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromStringInvalid() throws Exception {
        ExecutorConfig.fromString("localhost|ABC");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testFromStringNull() throws Exception {
        ExecutorConfig.fromString(null);
    }

}
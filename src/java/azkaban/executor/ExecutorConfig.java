package azkaban.executor;

/**
 * Class defines executor configuration
 */
public class ExecutorConfig {

    private String host;
    private Integer port;

    public ExecutorConfig(String host, Integer port) {
        super();
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    @Override
    public String toString() {
        return this.host + "|" + this.port;
    }

    public static ExecutorConfig fromString(String configStr) {

        if (null == configStr) {
            throw new IllegalArgumentException("configStr parameter can not be null");
        }

        ExecutorConfig config;

        try {

            String[] tokens = configStr.split("\\|");
            config = new ExecutorConfig(tokens[0], Integer.parseInt(tokens[1]));

        } catch (Exception e) {
            throw new IllegalArgumentException("Failed parsing configStr parameter", e);
        }

        return config;
    }

}

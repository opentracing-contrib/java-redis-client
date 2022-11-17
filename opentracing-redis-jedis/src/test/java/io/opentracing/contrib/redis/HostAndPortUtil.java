package io.opentracing.contrib.redis;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;

import java.util.ArrayList;
import java.util.List;

public final class HostAndPortUtil {
    private static List<HostAndPort> redisHostAndPortList = new ArrayList<HostAndPort>();
    private static List<HostAndPort> sentinelHostAndPortList = new ArrayList<HostAndPort>();
    private static List<HostAndPort> clusterHostAndPortList = new ArrayList<HostAndPort>();

    private static GenericContainer jedisClusterContainer = null;

    private HostAndPortUtil() {
        throw new InstantiationError("Must not instantiate this class");
    }

    static {
        redisHostAndPortList.add(new HostAndPort("localhost", Protocol.DEFAULT_PORT));
        redisHostAndPortList.add(new HostAndPort("localhost", Protocol.DEFAULT_PORT + 1));
        redisHostAndPortList.add(new HostAndPort("localhost", Protocol.DEFAULT_PORT + 2));
        redisHostAndPortList.add(new HostAndPort("localhost", Protocol.DEFAULT_PORT + 3));
        redisHostAndPortList.add(new HostAndPort("localhost", Protocol.DEFAULT_PORT + 4));
        redisHostAndPortList.add(new HostAndPort("localhost", Protocol.DEFAULT_PORT + 5));
        redisHostAndPortList.add(new HostAndPort("localhost", Protocol.DEFAULT_PORT + 6));

        sentinelHostAndPortList.add(new HostAndPort("localhost", Protocol.DEFAULT_SENTINEL_PORT));
        sentinelHostAndPortList.add(new HostAndPort("localhost", Protocol.DEFAULT_SENTINEL_PORT + 1));
        sentinelHostAndPortList.add(new HostAndPort("localhost", Protocol.DEFAULT_SENTINEL_PORT + 2));
        sentinelHostAndPortList.add(new HostAndPort("localhost", Protocol.DEFAULT_SENTINEL_PORT + 3));
//
//        clusterHostAndPortList.add(new HostAndPort("localhost", 7379));
//        clusterHostAndPortList.add(new HostAndPort("localhost", 7380));
//        clusterHostAndPortList.add(new HostAndPort("localhost", 7381));
//        clusterHostAndPortList.add(new HostAndPort("localhost", 7382));
//        clusterHostAndPortList.add(new HostAndPort("localhost", 7383));
//        clusterHostAndPortList.add(new HostAndPort("localhost", 7384));

        String envRedisHosts = System.getProperty("redis-hosts");
        String envSentinelHosts = System.getProperty("sentinel-hosts");
        String envClusterHosts = System.getProperty("cluster-hosts");

        redisHostAndPortList = parseHosts(envRedisHosts, redisHostAndPortList);
        sentinelHostAndPortList = parseHosts(envSentinelHosts, sentinelHostAndPortList);
        clusterHostAndPortList = parseHosts(envClusterHosts, clusterHostAndPortList);

        List<String> ports = new ArrayList<>();
        ports.add("7000:7000");
        ports.add("7001:7001");
        ports.add("7002:7002");
        ports.add("7003:7003");
        ports.add("7004:7004");
        jedisClusterContainer = new GenericContainer("grokzen/redis-cluster")
                .withEnv("IP", "0.0.0.0")
                .withExposedPorts(7000, 7001, 7002, 7003, 7004)
                .waitingFor(Wait.forLogMessage("[\\w\\W]*Cluster state changed: ok[\\w\\W]*", 6));

        ;
        jedisClusterContainer.setPortBindings(ports);
        jedisClusterContainer.start();

        clusterHostAndPortList.add(new HostAndPort(jedisClusterContainer.getHost(), 7000));
        clusterHostAndPortList.add(new HostAndPort(jedisClusterContainer.getHost(), 7001));
        clusterHostAndPortList.add(new HostAndPort(jedisClusterContainer.getHost(), 7002));
        clusterHostAndPortList.add(new HostAndPort(jedisClusterContainer.getHost(), 7003));
        clusterHostAndPortList.add(new HostAndPort(jedisClusterContainer.getHost(), 7004));

    }

    public static List<HostAndPort> parseHosts(String envHosts,
                                               List<HostAndPort> existingHostsAndPorts) {

        if (null != envHosts && 0 < envHosts.length()) {

            String[] hostDefs = envHosts.split(",");

            if (null != hostDefs && 2 <= hostDefs.length) {

                List<HostAndPort> envHostsAndPorts = new ArrayList<HostAndPort>(hostDefs.length);

                for (String hostDef : hostDefs) {

                    String[] hostAndPortParts = HostAndPort.extractParts(hostDef);

                    if (null != hostAndPortParts && 2 == hostAndPortParts.length) {
                        String host = hostAndPortParts[0];
                        int port = Protocol.DEFAULT_PORT;

                        try {
                            port = Integer.parseInt(hostAndPortParts[1]);
                        } catch (final NumberFormatException nfe) {
                        }

                    }
                }

                return envHostsAndPorts;
            }
        }

        return existingHostsAndPorts;
    }

    public static List<HostAndPort> getRedisServers() {
        return redisHostAndPortList;
    }

    public static List<HostAndPort> getSentinelServers() {
        return sentinelHostAndPortList;
    }

    public static List<HostAndPort> getClusterServers() {
        return clusterHostAndPortList;
    }
}

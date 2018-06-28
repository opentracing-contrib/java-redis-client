[![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov] [![Released Version][maven-img]][maven]


# OpenTracing Redis Client Instrumentation
OpenTracing instrumentation for Redis Client

## Requirements

- Java 8

## Installation

### Jedis

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-redis-jedis</artifactId>
    <version>0.0.2</version>
</dependency>
```

### Lettuce

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-redis-lettuce</artifactId>
    <version>0.0.2</version>
</dependency>
```

## Usage

```java

// Instantiate tracer
Tracer tracer = ...

```

### Span Name
By default, span names are set to the operation performed by the Jedis object. To customize the span name, provide a Function to the Jedis object that alters the span name. If a function is not provided, the span name will remain the default. Refer to the RedisSpanNameProvider class for a function that prefixes the operation name. 
```java
//Create Tracing Jedis with custom span name
Jedis jedis = new TracingJedis(tracer, false, RedisSpanNameProvider.PREFIX_OPERATION_NAME("redis.");
jedis.set("foo", "bar");
//Span name is now set to "redis.set"

```

### Jedis
```java

// Create Tracing Jedis
Jedis jedis = new TracingJedis(tracer, false);

jedis.set("foo", "bar");
String value = jedis.get("foo");

```

### Jedis Cluster
```java
Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
jedisClusterNodes.add(new HostAndPort("127.0.0.1", 7379));

// Create Tracing Jedis Cluster
JedisCluster jc = new TracingJedisCluster(jedisClusterNodes);
jc.set("foo", "bar");
String value = jc.get("foo");

```

### Jedis Pool
```java
// Configure the pool
JedisPoolConfig poolConfig = new JedisPoolConfig();
poolConfig.setMaxIdle(10);
poolConfig.setTestOnBorrow(false);

// Create Tracing Jedis Pool
JedisPool pool = new TracingJedisPool(poolConfig, "127.0.0.1", 6379, tracer, false);

try (Jedis jedis = pool.getResource()) {
    // jedis will be automatically closed and returned to the pool at the end of "try" block
    jedis.set("foo", "bar");
    String value = jedis.get("foo");
}
```

### Lettuce

```java

// Create client
RedisClient client = RedisClient.create("redis://localhost");

// Decorate StatefulRedisConnection with TracingStatefulRedisConnection
StatefulRedisConnection<String, String> connection = 
    new TracingStatefulRedisConnection(client.connect(), tracer, false);

// Get sync redis commands
RedisCommands<String, String> commands = connection.sync();

// Get async redis commands
RedisAsyncCommands<String, String> commandsAsync = connection.async();

```

## License

[Apache 2.0 License](./LICENSE).

[ci-img]: https://travis-ci.org/opentracing-contrib/java-redis-client.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-redis-client
[cov-img]: https://coveralls.io/repos/github/opentracing-contrib/java-redis-client/badge.svg?branch=master
[cov]: https://coveralls.io/github/opentracing-contrib/java-redis-client?branch=master
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-redis-parent.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-redis-parent

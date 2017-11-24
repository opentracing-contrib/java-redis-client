[![Build Status][ci-img]][ci] [![Released Version][maven-img]][maven]


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
    <version>0.0.1</version>
</dependency>
```

### Lettuce

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-redis-lettuce</artifactId>
    <version>0.0.1</version>
</dependency>
```

## Usage

```java

// Instantiate tracer
Tracer tracer = ...

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


[ci-img]: https://travis-ci.org/opentracing-contrib/java-redis-client.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-redis-client
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-redis-client.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-redis-client

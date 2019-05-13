[![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov] [![Released Version][maven-img]][maven] [![Apache-2.0 license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


# OpenTracing Redis Client Instrumentation
OpenTracing instrumentation for Redis Client

## Requirements

- Java 8

## Installation

### Jedis

#### Jedis 2

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-redis-jedis</artifactId>
    <version>VERSION</version>
</dependency>
```

#### Jedis 3

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-redis-jedis3</artifactId>
    <version>VERSION</version>
</dependency>
```

### Lettuce

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-redis-lettuce</artifactId>
    <version>VERSION</version>
</dependency>
```

### Redisson
pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-redis-redisson</artifactId>
    <version>VERSION</version>
</dependency>
```

### Spring Data Redis 1.x

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-redis-spring-data</artifactId>
    <version>VERSION</version>
</dependency>
```

### Spring Data Redis 2.x

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-redis-spring-data2</artifactId>
    <version>VERSION</version>
</dependency>
```

## Usage

```java

// Instantiate tracer
Tracer tracer = ...

// Create TracingConfiguration
TracingConfiguration tracingConfiguration = new TracingConfiguration.Builder(tracer).build(); 

```

### Jedis
```java

// Create Tracing Jedis
Jedis jedis = new TracingJedis(tracingConfiguration);

jedis.set("foo", "bar");
String value = jedis.get("foo");

```

### Jedis Cluster
```java
Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
jedisClusterNodes.add(new HostAndPort("127.0.0.1", 7379));

// Create Tracing Jedis Cluster
JedisCluster jc = new TracingJedisCluster(jedisClusterNodes, tracingConfiguration);
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
JedisPool pool = new TracingJedisPool(poolConfig, "127.0.0.1", 6379, tracingConfiguration);

try (Jedis jedis = pool.getResource()) {
    // jedis will be automatically closed and returned to the pool at the end of "try" block
    jedis.set("foo", "bar");
    String value = jedis.get("foo");
}
```

### Jedis Sentinel Pool
```java
// Create Tracing Jedis Sentinel Pool
JedisSentinelPool pool = new TracingJedisSentinelPool(tracingConfiguration, MASTER_NAME, sentinels, poolConfig);

try (Jedis jedis = pool.getResource()) {
// jedis will be automatically closed and returned to the pool at the end of "try" block
   jedis.set("foo", "bar"));
   String value = jedis.get("foo"));
}
```

### Jedis Span Name
By default, span names are set to the operation performed by the Jedis object. 
To customize the span name, provide a Function to the TracingConfiguration object that alters the span name. 
If a function is not provided, the span name will remain the default. 
Refer to the RedisSpanNameProvider class for a function that prefixes the operation name. 
```java
TracingConfiguration tracingConfiguration = new TracingConfiguration.Builder(tracer)
    .withSpanNameProvider(RedisSpanNameProvider.PREFIX_OPERATION_NAME("redis."))
    .build(); 
//Create Tracing Jedis with custom span name
Jedis jedis = new TracingJedis(tracingConfiguration);
jedis.set("foo", "bar");
//Span name is now set to "redis.set"

```

### Lettuce

```java

// Create client
RedisClient client = RedisClient.create("redis://localhost");

// Decorate StatefulRedisConnection with TracingStatefulRedisConnection
StatefulRedisConnection<String, String> connection = 
    new TracingStatefulRedisConnection(client.connect(), tracingConfiguration);

// Get sync redis commands
RedisCommands<String, String> commands = connection.sync();

// Get async redis commands
RedisAsyncCommands<String, String> commandsAsync = connection.async();

```

### Redisson

```java
// Instantiate tracer
Tracer tracer = ...

// Create Redisson config object
Config = ...

// Create Redisson instance
RedissonClient redissonClient = Redisson.create(config);

// Decorate RedissonClient with TracingRedissonClient
RedissonClient tracingRedissonClient =  new TracingRedissonClient(redissonClient, tracer);

// Get object you need using TracingRedissonClient
RMap<MyKey, MyValue> map = tracingRedissonClient.getMap("myMap");
```

### Spring

```java

// Create tracing connection factory bean
@Bean
public RedisConnectionFactory redisConnectionFactory() {
    LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory();
    lettuceConnectionFactory.afterPropertiesSet();
    return new TracingRedisConnectionFactory(lettuceConnectionFactory,
        new TracingConfiguration.Builder(tracer()).build());
}
```

Note: if you use Lettuce/Jedis you could achieve the same result using the Lettuce/Jedis support when 
configuring LettuceConnectionFactory/JedisConnectionFactory instead of using a wrapping TracingRedisConnectionFactory.

## License

[Apache 2.0 License](./LICENSE).

[ci-img]: https://travis-ci.org/opentracing-contrib/java-redis-client.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-redis-client
[cov-img]: https://coveralls.io/repos/github/opentracing-contrib/java-redis-client/badge.svg?branch=master
[cov]: https://coveralls.io/github/opentracing-contrib/java-redis-client?branch=master
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-redis-parent.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-redis-parent

package io.opentracing.contrib.redis.spring.connection;

import io.opentracing.Tracer;
import org.springframework.data.redis.connection.ReactiveClusterGeoCommands;
import org.springframework.data.redis.connection.ReactiveClusterHashCommands;
import org.springframework.data.redis.connection.ReactiveClusterHyperLogLogCommands;
import org.springframework.data.redis.connection.ReactiveClusterKeyCommands;
import org.springframework.data.redis.connection.ReactiveClusterListCommands;
import org.springframework.data.redis.connection.ReactiveClusterNumberCommands;
import org.springframework.data.redis.connection.ReactiveClusterServerCommands;
import org.springframework.data.redis.connection.ReactiveClusterSetCommands;
import org.springframework.data.redis.connection.ReactiveClusterStringCommands;
import org.springframework.data.redis.connection.ReactiveClusterZSetCommands;
import org.springframework.data.redis.connection.ReactiveRedisClusterConnection;
import org.springframework.data.redis.connection.ReactiveScriptingCommands;
import org.springframework.data.redis.connection.RedisClusterNode;
import reactor.core.publisher.Mono;

public class TracingReactiveRedisClusterConnection implements ReactiveRedisClusterConnection {
  private final ReactiveRedisClusterConnection reactiveRedisClusterConnection;
  private final boolean withActiveSpanOnly;
  private final Tracer tracer;

  public TracingReactiveRedisClusterConnection(
      ReactiveRedisClusterConnection reactiveRedisClusterConnection, boolean withActiveSpanOnly,
      Tracer tracer) {
    this.reactiveRedisClusterConnection = reactiveRedisClusterConnection;
    this.withActiveSpanOnly = withActiveSpanOnly;
    this.tracer = tracer;
  }

  @Override
  public ReactiveClusterKeyCommands keyCommands() {
    return reactiveRedisClusterConnection.keyCommands();
  }

  @Override
  public ReactiveClusterStringCommands stringCommands() {
    return reactiveRedisClusterConnection.stringCommands();
  }

  @Override
  public ReactiveClusterNumberCommands numberCommands() {
    return reactiveRedisClusterConnection.numberCommands();
  }

  @Override
  public ReactiveClusterListCommands listCommands() {
    return reactiveRedisClusterConnection.listCommands();
  }

  @Override
  public ReactiveClusterSetCommands setCommands() {
    return reactiveRedisClusterConnection.setCommands();
  }

  @Override
  public ReactiveClusterZSetCommands zSetCommands() {
    return reactiveRedisClusterConnection.zSetCommands();
  }

  @Override
  public ReactiveClusterHashCommands hashCommands() {
    return reactiveRedisClusterConnection.hashCommands();
  }

  @Override
  public ReactiveClusterGeoCommands geoCommands() {
    return reactiveRedisClusterConnection.geoCommands();
  }

  @Override
  public ReactiveClusterHyperLogLogCommands hyperLogLogCommands() {
    return reactiveRedisClusterConnection.hyperLogLogCommands();
  }

  @Override
  public ReactiveClusterServerCommands serverCommands() {
    return reactiveRedisClusterConnection.serverCommands();
  }

  @Override
  public Mono<String> ping(RedisClusterNode node) {
    return RedisTracingUtils.doInScope(RedisCommand.PING,
        () -> reactiveRedisClusterConnection.ping(node),
        withActiveSpanOnly, tracer);
  }

  @Override
  public void close() {
    reactiveRedisClusterConnection.close();
  }

  @Override
  public ReactiveScriptingCommands scriptingCommands() {
    return reactiveRedisClusterConnection.scriptingCommands();
  }

  @Override
  public Mono<String> ping() {
    return RedisTracingUtils.doInScope(RedisCommand.PING, reactiveRedisClusterConnection::ping,
        withActiveSpanOnly, tracer);
  }
}

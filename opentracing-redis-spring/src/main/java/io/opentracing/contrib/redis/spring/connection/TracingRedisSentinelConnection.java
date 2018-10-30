/*
 * Copyright 2017-2018 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.opentracing.contrib.redis.spring.connection;

import static io.opentracing.contrib.redis.spring.connection.RedisTracingUtils.doInScope;

import io.opentracing.Tracer;
import io.opentracing.contrib.redis.common.TracingHelper;
import java.io.IOException;
import java.util.Collection;
import org.springframework.data.redis.connection.NamedNode;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisServer;

public class TracingRedisSentinelConnection implements RedisSentinelConnection {
  private final RedisSentinelConnection redisSentinelConnection;
  private final boolean withActiveSpanOnly;
  private final Tracer tracer;

  public TracingRedisSentinelConnection(
      RedisSentinelConnection redisSentinelConnection, boolean withActiveSpanOnly,
      Tracer tracer) {
    this.redisSentinelConnection = redisSentinelConnection;
    this.withActiveSpanOnly = withActiveSpanOnly;
    this.tracer = tracer;
  }

  @Override
  public boolean isOpen() {
    return doInScope("isOpen", redisSentinelConnection::isOpen, withActiveSpanOnly, tracer);
  }

  @Override
  public void failover(NamedNode master) {
    TracingHelper.doInScope("failover", () -> redisSentinelConnection.failover(master),
        withActiveSpanOnly, tracer);
  }

  @Override
  public Collection<RedisServer> masters() {
    return TracingHelper.doInScope("masters", redisSentinelConnection::masters,
        withActiveSpanOnly, tracer);
  }

  @Override
  public Collection<RedisServer> slaves(
      NamedNode master) {
    return TracingHelper.doInScope("slaves", () -> redisSentinelConnection.slaves(master),
        withActiveSpanOnly, tracer);
  }

  @Override
  public void remove(NamedNode master) {
    TracingHelper.doInScope("remove", () -> redisSentinelConnection.remove(master),
        withActiveSpanOnly, tracer);
  }

  @Override
  public void monitor(RedisServer master) {
    TracingHelper.doInScope("monitor", () -> redisSentinelConnection.monitor(master),
        withActiveSpanOnly, tracer);
  }

  @Override
  public void close() throws IOException {
    TracingHelper.doInScopeExceptionally("close", redisSentinelConnection::close,
        withActiveSpanOnly, tracer);
  }
}

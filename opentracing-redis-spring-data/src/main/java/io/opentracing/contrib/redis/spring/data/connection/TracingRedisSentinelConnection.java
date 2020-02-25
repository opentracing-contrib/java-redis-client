/*
 * Copyright 2017-2020 The OpenTracing Authors
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
package io.opentracing.contrib.redis.spring.data.connection;

import io.opentracing.Span;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.contrib.redis.common.TracingHelper;
import java.io.IOException;
import java.util.Collection;
import org.springframework.data.redis.connection.NamedNode;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisServer;

public class TracingRedisSentinelConnection implements RedisSentinelConnection {
  private final RedisSentinelConnection redisSentinelConnection;
  private final TracingHelper helper;

  public TracingRedisSentinelConnection(RedisSentinelConnection redisSentinelConnection,
      TracingConfiguration configuration) {
    this.redisSentinelConnection = redisSentinelConnection;
    this.helper = new TracingHelper(configuration);
  }

  @Override
  public boolean isOpen() {
    Span span = helper.buildSpan("isOpen");
    return helper.decorate(span, redisSentinelConnection::isOpen);
  }

  @Override
  public void failover(NamedNode master) {
    Span span = helper.buildSpan("failover");
    helper.decorate(span, () -> redisSentinelConnection.failover(master));
  }

  @Override
  public Collection<RedisServer> masters() {
    Span span = helper.buildSpan("masters");
    return helper.decorate(span, redisSentinelConnection::masters);
  }

  @Override
  public Collection<RedisServer> slaves(NamedNode master) {
    Span span = helper.buildSpan("slaves");
    return helper.decorate(span, () -> redisSentinelConnection.slaves(master));
  }

  @Override
  public void remove(NamedNode master) {
    Span span = helper.buildSpan("remove");
    helper.decorate(span, () -> redisSentinelConnection.remove(master));
  }

  @Override
  public void monitor(RedisServer master) {
    Span span = helper.buildSpan("monitor");
    helper.decorate(span, () -> redisSentinelConnection.monitor(master));
  }

  @Override
  public void close() throws IOException {
    Span span = helper.buildSpan("close");
    helper.decorateThrowing(span, () -> redisSentinelConnection.close());
  }
}

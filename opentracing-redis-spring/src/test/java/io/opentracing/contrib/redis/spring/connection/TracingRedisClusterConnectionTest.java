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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.opentracing.mock.MockTracer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisClusterCommands;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Daniel del Castillo
 */
@ContextConfiguration(classes = {MockConfiguration.class})
@RunWith(SpringJUnit4ClassRunner.class)
public class TracingRedisClusterConnectionTest extends TracingRedisConnectionTest {
  @Autowired
  MockTracer tracer;

  @Autowired
  RedisConnectionFactory connectionFactory;

  @Autowired
  RedisClusterConnection mockRedisClusterConnection;

  private final RedisClusterNode mockRedisClusterNode = mock(RedisClusterNode.class);

  @Override
  protected RedisClusterConnection mockRedisConnection() {
    return mockRedisClusterConnection;
  }

  @Override
  protected RedisClusterConnection getConnection() {
    return connectionFactory.getClusterConnection();
  }


  private void commandCreatesNewSpan(String commandName, Runnable command) {
    AssertionUtils.commandCreatesNewSpan(tracer, commandName, command);
  }

  private void commandSpanJoinsActiveSpan(Runnable command) {
    AssertionUtils.commandSpanJoinsActiveSpan(tracer, command);
  }

  /**
   * Make sure we get a {@link RedisConnectionFactory} that returns a tracing cluster connection. Without
   * our Aspect, this would return a regular {@link RedisClusterConnection}.
   */
  @Override
  @Test
  public void connectionIsInstrumented() {
    assertTrue(connectionFactory.getClusterConnection() instanceof TracingRedisClusterConnection);
  }

  /*
   * The test below verify the creation of a new Span
   */

  @Test
  public void invokingClusterGetNodes() {
    commandCreatesNewSpan(RedisCommand.CLUSTER_NODES, () -> getConnection().clusterGetNodes());
    verify(mockRedisConnection()).clusterGetNodes();
  }

  @Test
  public void invokingClusterGetSlaves() {
    commandCreatesNewSpan(RedisCommand.CLUSTER_SLAVES,
        () -> getConnection().clusterGetSlaves(mockRedisClusterNode));
    verify(mockRedisConnection()).clusterGetSlaves(mockRedisClusterNode);
  }

  @Test
  public void invokingClusterGetMasterSlaveMap() {
    commandCreatesNewSpan(RedisCommand.CLUSTER_MASTER_SLAVE_MAP,
        () -> getConnection().clusterGetMasterSlaveMap());
    verify(mockRedisConnection()).clusterGetMasterSlaveMap();
  }

  @Test
  public void invokingClusterGetSlotForKey() {
    commandCreatesNewSpan(RedisCommand.CLUSTER_KEYSLOT,
        () -> getConnection().clusterGetSlotForKey("key".getBytes()));
    verify(mockRedisConnection()).clusterGetSlotForKey("key".getBytes());
  }

  @Test
  public void invokingClusterGetNodeForSlot() {
    commandCreatesNewSpan(RedisCommand.CLUSTER_NODE_FOR_SLOT,
        () -> getConnection().clusterGetNodeForSlot(0));
    verify(mockRedisConnection()).clusterGetNodeForSlot(0);
  }

  @Test
  public void invokingClusterGetNodeForKey() {
    commandCreatesNewSpan(RedisCommand.CLUSTER_NODE_FOR_KEY,
        () -> getConnection().clusterGetNodeForKey("key".getBytes()));
    verify(mockRedisConnection()).clusterGetNodeForKey("key".getBytes());
  }

  @Test
  public void invokingClusterGetClusterInfo() {
    commandCreatesNewSpan(RedisCommand.CLUSTER_INFO, () -> getConnection().clusterGetClusterInfo());
    verify(mockRedisConnection()).clusterGetClusterInfo();
  }

  @Test
  public void invokingClusterAddSlots() {
    commandCreatesNewSpan(RedisCommand.CLUSTER_ADDSLOTS,
        () -> getConnection().clusterAddSlots(mockRedisClusterNode, 0));
    verify(mockRedisClusterConnection).clusterAddSlots(mockRedisClusterNode, 0);

    RedisClusterNode.SlotRange range = RedisClusterNode.SlotRange.empty();
    commandCreatesNewSpan(RedisCommand.CLUSTER_ADDSLOTS,
        () -> getConnection().clusterAddSlots(mockRedisClusterNode, range));
    verify(mockRedisConnection()).clusterAddSlots(mockRedisClusterNode, range);
  }

  @Test
  public void invokingClusterCountKeysInSlot() {
    commandCreatesNewSpan(RedisCommand.CLUSTER_COUNTKEYSINSLOT,
        () -> getConnection().clusterCountKeysInSlot(0));
    verify(mockRedisConnection()).clusterCountKeysInSlot(0);
  }

  @Test
  public void invokingClusterDeleteSlots() {
    commandCreatesNewSpan(RedisCommand.CLUSTER_DELSLOTS,
        () -> getConnection().clusterDeleteSlots(mockRedisClusterNode, 0));
    verify(mockRedisConnection()).clusterDeleteSlots(mockRedisClusterNode, 0);
  }

  @Test
  public void invokingClusterDeleteSlotsInRange() {
    RedisClusterNode.SlotRange range = RedisClusterNode.SlotRange.empty();
    commandCreatesNewSpan(RedisCommand.CLUSTER_DELSLOTS,
        () -> getConnection().clusterDeleteSlotsInRange(mockRedisClusterNode, range));
    verify(mockRedisConnection()).clusterDeleteSlotsInRange(mockRedisClusterNode, range);
  }

  @Test
  public void invokingClusterForget() {
    commandCreatesNewSpan(RedisCommand.CLUSTER_FORGET,
        () -> getConnection().clusterForget(mockRedisClusterNode));
    verify(mockRedisConnection()).clusterForget(mockRedisClusterNode);
  }

  @Test
  public void invokingClusterMeet() {
    commandCreatesNewSpan(RedisCommand.CLUSTER_MEET,
        () -> getConnection().clusterMeet(mockRedisClusterNode));
    verify(mockRedisConnection()).clusterMeet(mockRedisClusterNode);
  }

  @Test
  public void invokingClusterSetSlot() {
    commandCreatesNewSpan(RedisCommand.CLUSTER_SETSLOT,
        () -> getConnection().clusterSetSlot(mockRedisClusterNode, 0,
            RedisClusterCommands.AddSlots.MIGRATING));
    verify(mockRedisConnection())
        .clusterSetSlot(mockRedisClusterNode, 0, RedisClusterCommands.AddSlots.MIGRATING);
  }

  @Test
  public void invokingClusterGetKeysInSlot() {
    commandCreatesNewSpan(RedisCommand.CLUSTER_GETKEYSINSLOT,
        () -> getConnection().clusterGetKeysInSlot(0, 1));
    verify(mockRedisConnection()).clusterGetKeysInSlot(0, 1);
  }

  @Test
  public void invokingClusterReplicate() {
    RedisClusterNode slaveClusterNode = mock(RedisClusterNode.class);
    commandCreatesNewSpan(RedisCommand.CLUSTER_REPLICATE,
        () -> getConnection().clusterReplicate(mockRedisClusterNode,
            slaveClusterNode));
    verify(mockRedisConnection()).clusterReplicate(mockRedisClusterNode, slaveClusterNode);
  }

  @Test
  public void invokingPing() {
    commandCreatesNewSpan(RedisCommand.PING, () -> getConnection().ping(mockRedisClusterNode));
    verify(mockRedisConnection()).ping(mockRedisClusterNode);
  }

  @Test
  public void invokingBgReWriteAof() {
    commandCreatesNewSpan(RedisCommand.BGREWRITEAOF,
        () -> getConnection().bgReWriteAof(mockRedisClusterNode));
    verify(mockRedisConnection()).bgReWriteAof(mockRedisClusterNode);
  }

  @Test
  public void invokingBgSave() {
    commandCreatesNewSpan(RedisCommand.BGSAVE, () -> getConnection().bgSave(mockRedisClusterNode));
    verify(mockRedisConnection()).bgSave(mockRedisClusterNode);
  }

  @Test
  public void invokingLastSave() {
    commandCreatesNewSpan(RedisCommand.LASTSAVE,
        () -> getConnection().lastSave(mockRedisClusterNode));
    verify(mockRedisConnection()).lastSave(mockRedisClusterNode);
  }

  @Test
  public void invokingSave() {
    commandCreatesNewSpan(RedisCommand.SAVE, () -> getConnection().save(mockRedisClusterNode));
    verify(mockRedisConnection()).save(mockRedisClusterNode);
  }

  @Test
  public void invokingDbSize() {
    commandCreatesNewSpan(RedisCommand.DBSIZE, () -> getConnection().dbSize(mockRedisClusterNode));
    verify(mockRedisConnection()).dbSize(mockRedisClusterNode);
  }

  @Test
  public void invokingFlushDb() {
    commandCreatesNewSpan(RedisCommand.FLUSHDB,
        () -> getConnection().flushDb(mockRedisClusterNode));
    verify(mockRedisConnection()).flushDb(mockRedisClusterNode);
  }

  @Test
  public void invokingFlushAll() {
    commandCreatesNewSpan(RedisCommand.FLUSHALL,
        () -> getConnection().flushAll(mockRedisClusterNode));
    verify(mockRedisConnection()).flushAll(mockRedisClusterNode);
  }

  @Test
  public void invokingInfo() {
    commandCreatesNewSpan(RedisCommand.INFO, () -> getConnection().info(mockRedisClusterNode));
    verify(mockRedisConnection()).info(mockRedisClusterNode);

    commandCreatesNewSpan(RedisCommand.INFO,
        () -> getConnection().info(mockRedisClusterNode, "section"));
    verify(mockRedisConnection()).info(mockRedisClusterNode, "section");
  }

  @Test
  public void invokingKeys() {
    commandCreatesNewSpan(RedisCommand.KEYS,
        () -> getConnection().keys(mockRedisClusterNode, "pattern".getBytes()));
    verify(mockRedisConnection()).keys(mockRedisClusterNode, "pattern".getBytes());
  }

  @Test
  public void invokingRandomKey() {
    commandCreatesNewSpan(RedisCommand.RANDOMKEY,
        () -> getConnection().randomKey(mockRedisClusterNode));
    verify(mockRedisConnection()).randomKey(mockRedisClusterNode);
  }

  @Test
  public void invokingShutdown() {
    commandCreatesNewSpan(RedisCommand.SHUTDOWN,
        () -> getConnection().shutdown(mockRedisClusterNode));
    verify(mockRedisConnection()).shutdown(mockRedisClusterNode);
  }

  @Test
  public void invokingGetConfig() {
    commandCreatesNewSpan(RedisCommand.CONFIG_GET,
        () -> getConnection().getConfig(mockRedisClusterNode, "pattern"));
    verify(mockRedisConnection()).getConfig(mockRedisClusterNode, "pattern");
  }

  @Test
  public void invokingSetConfig() {
    commandCreatesNewSpan(RedisCommand.CONFIG_SET,
        () -> getConnection().setConfig(mockRedisClusterNode, "param", "val"));
    verify(mockRedisConnection()).setConfig(mockRedisClusterNode, "param", "val");
  }

  @Test
  public void invokingResetConfigStats() {
    commandCreatesNewSpan(RedisCommand.CONFIG_RESETSTAT,
        () -> getConnection().resetConfigStats(mockRedisClusterNode));
    verify(mockRedisConnection()).resetConfigStats(mockRedisClusterNode);
  }

  @Test
  public void invokingTime() {
    commandCreatesNewSpan(RedisCommand.TIME, () -> getConnection().time(mockRedisClusterNode));
    verify(mockRedisConnection()).time(mockRedisClusterNode);
  }

  @Test
  public void invokingGetClientList() {
    commandCreatesNewSpan(RedisCommand.CLIENT_LIST,
        () -> getConnection().getClientList(mockRedisClusterNode));
    verify(mockRedisConnection()).getClientList(mockRedisClusterNode);
  }

  /*
   * The test below verify the creation of a child Span. We only do it for one invocation as they all use the same code.
   */

  @Test
  public void clusterSpanJoinsActiveSpan() {
    commandSpanJoinsActiveSpan(() -> getConnection().clusterMeet(mockRedisClusterNode));
  }

}

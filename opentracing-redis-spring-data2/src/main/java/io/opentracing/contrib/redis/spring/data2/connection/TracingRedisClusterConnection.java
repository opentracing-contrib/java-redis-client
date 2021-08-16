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
package io.opentracing.contrib.redis.spring.data2.connection;

import static io.opentracing.contrib.redis.common.RedisCommand.BGREWRITEAOF;
import static io.opentracing.contrib.redis.common.RedisCommand.BGSAVE;
import static io.opentracing.contrib.redis.common.RedisCommand.CLIENT_LIST;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_ADDSLOTS;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_COUNTKEYSINSLOT;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_DELSLOTS;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_FORGET;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_GETKEYSINSLOT;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_INFO;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_KEYSLOT;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_MASTER_SLAVE_MAP;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_MEET;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_NODES;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_NODE_FOR_KEY;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_NODE_FOR_SLOT;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_REPLICATE;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_SETSLOT;
import static io.opentracing.contrib.redis.common.RedisCommand.CLUSTER_SLAVES;
import static io.opentracing.contrib.redis.common.RedisCommand.CONFIG_GET;
import static io.opentracing.contrib.redis.common.RedisCommand.CONFIG_RESETSTAT;
import static io.opentracing.contrib.redis.common.RedisCommand.CONFIG_REWRITE;
import static io.opentracing.contrib.redis.common.RedisCommand.CONFIG_SET;
import static io.opentracing.contrib.redis.common.RedisCommand.DBSIZE;
import static io.opentracing.contrib.redis.common.RedisCommand.EXECUTE;
import static io.opentracing.contrib.redis.common.RedisCommand.FLUSHALL;
import static io.opentracing.contrib.redis.common.RedisCommand.FLUSHDB;
import static io.opentracing.contrib.redis.common.RedisCommand.INFO;
import static io.opentracing.contrib.redis.common.RedisCommand.KEYS;
import static io.opentracing.contrib.redis.common.RedisCommand.LASTSAVE;
import static io.opentracing.contrib.redis.common.RedisCommand.PING;
import static io.opentracing.contrib.redis.common.RedisCommand.RANDOMKEY;
import static io.opentracing.contrib.redis.common.RedisCommand.SAVE;
import static io.opentracing.contrib.redis.common.RedisCommand.SCAN;
import static io.opentracing.contrib.redis.common.RedisCommand.SHUTDOWN;
import static io.opentracing.contrib.redis.common.RedisCommand.TIME;

import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.contrib.redis.common.TracingHelper;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisClusterServerCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.RedisClientInfo;


/**
 * OpenTracing instrumentation of a {@link RedisClusterConnection}.
 *
 * @author Daniel del Castillo
 */
public class TracingRedisClusterConnection extends TracingRedisConnection
    implements RedisClusterConnection {

  private final RedisClusterConnection connection;
  private final TracingHelper helper;

  public TracingRedisClusterConnection(RedisClusterConnection connection,
      TracingConfiguration tracingConfiguration) {
    super(connection, tracingConfiguration);
    this.connection = connection;
    this.helper = new TracingHelper(tracingConfiguration);
  }

  @Override
  public RedisClusterServerCommands serverCommands() {
    return connection.serverCommands();
  }

  @Override
  public Iterable<RedisClusterNode> clusterGetNodes() {
    return helper.doInScope(CLUSTER_NODES, () -> connection.clusterGetNodes());
  }

  @Override
  public Collection<RedisClusterNode> clusterGetSlaves(RedisClusterNode master) {
    return helper.doInScope(CLUSTER_SLAVES, () -> connection.clusterGetSlaves(master));
  }

  @Override
  public Map<RedisClusterNode, Collection<RedisClusterNode>> clusterGetMasterSlaveMap() {
    return helper.doInScope(CLUSTER_MASTER_SLAVE_MAP, () -> connection.clusterGetMasterSlaveMap());
  }

  @Override
  public Integer clusterGetSlotForKey(byte[] key) {
    return helper.doInScope(CLUSTER_KEYSLOT, () -> connection.clusterGetSlotForKey(key));
  }

  @Override
  public RedisClusterNode clusterGetNodeForSlot(int slot) {
    return helper.doInScope(CLUSTER_NODE_FOR_SLOT, () -> connection.clusterGetNodeForSlot(slot));
  }

  @Override
  public RedisClusterNode clusterGetNodeForKey(byte[] key) {
    return helper.doInScope(CLUSTER_NODE_FOR_KEY, () -> connection.clusterGetNodeForKey(key));
  }

  @Override
  public ClusterInfo clusterGetClusterInfo() {
    return helper.doInScope(CLUSTER_INFO, () -> connection.clusterGetClusterInfo());
  }

  @Override
  public void clusterAddSlots(RedisClusterNode node, int... slots) {
    helper.doInScope(CLUSTER_ADDSLOTS, () -> connection.clusterAddSlots(node, slots));
  }

  @Override
  public void clusterAddSlots(RedisClusterNode node, SlotRange range) {
    helper.doInScope(CLUSTER_ADDSLOTS, () -> connection.clusterAddSlots(node, range));
  }

  @Override
  public Long clusterCountKeysInSlot(int slot) {
    return helper.doInScope(CLUSTER_COUNTKEYSINSLOT, () -> connection.clusterCountKeysInSlot(slot));
  }

  @Override
  public void clusterDeleteSlots(RedisClusterNode node, int... slots) {
    helper.doInScope(CLUSTER_DELSLOTS, () -> connection.clusterDeleteSlots(node, slots));
  }

  @Override
  public void clusterDeleteSlotsInRange(RedisClusterNode node, SlotRange range) {
    helper.doInScope(CLUSTER_DELSLOTS, () -> connection.clusterDeleteSlotsInRange(node, range));
  }

  @Override
  public void clusterForget(RedisClusterNode node) {
    helper.doInScope(CLUSTER_FORGET, () -> connection.clusterForget(node));
  }

  @Override
  public void clusterMeet(RedisClusterNode node) {
    helper.doInScope(CLUSTER_MEET, () -> connection.clusterMeet(node));
  }

  @Override
  public void clusterSetSlot(RedisClusterNode node, int slot, AddSlots mode) {
    helper.doInScope(CLUSTER_SETSLOT, () -> connection.clusterSetSlot(node, slot, mode));
  }

  @Override
  public List<byte[]> clusterGetKeysInSlot(int slot, Integer count) {
    return helper
        .doInScope(CLUSTER_GETKEYSINSLOT, () -> connection.clusterGetKeysInSlot(slot, count));
  }

  @Override
  public void clusterReplicate(RedisClusterNode master, RedisClusterNode slave) {
    helper.doInScope(CLUSTER_REPLICATE, () -> connection.clusterReplicate(master, slave));
  }

  @Override
  public String ping(RedisClusterNode node) {
    return helper.doInScope(PING, () -> connection.ping(node));
  }

  @Override
  public void bgReWriteAof(RedisClusterNode node) {
    helper.doInScope(BGREWRITEAOF, () -> connection.bgReWriteAof(node));
  }

  @Override
  public void bgSave(RedisClusterNode node) {
    helper.doInScope(BGSAVE, () -> connection.bgSave(node));
  }

  @Override
  public Long lastSave(RedisClusterNode node) {
    return helper.doInScope(LASTSAVE, () -> connection.lastSave(node));
  }

  @Override
  public void save(RedisClusterNode node) {
    helper.doInScope(SAVE, () -> connection.save(node));
  }

  @Override
  public Long dbSize(RedisClusterNode node) {
    return helper.doInScope(DBSIZE, () -> connection.dbSize(node));
  }

  @Override
  public void flushDb(RedisClusterNode node) {
    helper.doInScope(FLUSHDB, () -> connection.flushDb(node));
  }

  @Override
  public void flushAll(RedisClusterNode node) {
    helper.doInScope(FLUSHALL, () -> connection.flushAll(node));
  }

  @Override
  public Properties info(RedisClusterNode node) {
    return helper.doInScope(INFO, () -> connection.info(node));
  }

  @Override
  public Properties info(RedisClusterNode node, String section) {
    return helper.doInScope(INFO, () -> connection.info(node, section));
  }

  @Override
  public Set<byte[]> keys(RedisClusterNode node, byte[] pattern) {
    return helper.doInScope(KEYS, () -> connection.keys(node, pattern));
  }

  @Override
  public Cursor<byte[]> scan(RedisClusterNode node, ScanOptions options) {
    return helper.doInScope(SCAN, () -> connection.scan(node, options));
  }

  @Override
  public byte[] randomKey(RedisClusterNode node) {
    return helper.doInScope(RANDOMKEY, () -> connection.randomKey(node));
  }

  @Override
  public <T> T execute(String command, byte[] key, Collection<byte[]> args) {
    return helper.doInScope(EXECUTE, () -> connection.execute(command, key, args));
  }

  @Override
  public void shutdown(RedisClusterNode node) {
    helper.doInScope(SHUTDOWN, () -> connection.shutdown(node));
  }

  @Override
  public Properties getConfig(RedisClusterNode node, String pattern) {
    return helper.doInScope(CONFIG_GET, () -> connection.getConfig(node, pattern));
  }

  @Override
  public void setConfig(RedisClusterNode node, String param, String value) {
    helper.doInScope(CONFIG_SET, () -> connection.setConfig(node, param, value));
  }

  @Override
  public void resetConfigStats(RedisClusterNode node) {
    helper.doInScope(CONFIG_RESETSTAT, () -> connection.resetConfigStats(node));
  }

  @Override
  public void rewriteConfig(RedisClusterNode node) {
    helper.doInScope(CONFIG_REWRITE, () -> connection.rewriteConfig(node));
  }

  @Override
  public Long time(RedisClusterNode node) {
    return helper.doInScope(TIME, () -> connection.time(node));
  }

  @Override
  public Long time(RedisClusterNode node, TimeUnit timeUnit) {
    return helper.doInScope(TIME, () -> connection.time(node, timeUnit));
  }

  @Override
  public List<RedisClientInfo> getClientList(RedisClusterNode node) {
    return helper.doInScope(CLIENT_LIST, () -> connection.getClientList(node));
  }

}

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

import static io.opentracing.contrib.redis.spring.connection.RedisCommand.BGREWRITEAOF;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.BGSAVE;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLIENT_LIST;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_ADDSLOTS;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_COUNTKEYSINSLOT;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_DELSLOTS;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_FORGET;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_GETKEYSINSLOT;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_INFO;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_KEYSLOT;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_MASTER_SLAVE_MAP;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_MEET;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_NODES;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_NODE_FOR_KEY;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_NODE_FOR_SLOT;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_REPLICATE;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_SETSLOT;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CLUSTER_SLAVES;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CONFIG_GET;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CONFIG_RESETSTAT;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.CONFIG_SET;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.DBSIZE;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.FLUSHALL;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.FLUSHDB;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.INFO;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.KEYS;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.LASTSAVE;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.PING;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.RANDOMKEY;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.SAVE;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.SHUTDOWN;
import static io.opentracing.contrib.redis.spring.connection.RedisCommand.TIME;

import io.opentracing.Tracer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisClusterServerCommands;
import org.springframework.data.redis.core.types.RedisClientInfo;


/**
 * OpenTracing instrumentation of a {@link RedisClusterConnection}.
 *
 * @author Daniel del Castillo
 */
public class TracingRedisClusterConnection extends TracingRedisConnection
    implements RedisClusterConnection {

  private final RedisClusterConnection connection;

  public TracingRedisClusterConnection(RedisClusterConnection connection,
      boolean withActiveSpanOnly, Tracer tracer) {
    super(connection, withActiveSpanOnly, tracer);
    this.connection = connection;
  }

  @Override
  public RedisClusterServerCommands serverCommands() {
    return connection.serverCommands();
  }

  @Override
  public Iterable<RedisClusterNode> clusterGetNodes() {
    return doInScope(CLUSTER_NODES, () -> connection.clusterGetNodes());
  }

  @Override
  public Collection<RedisClusterNode> clusterGetSlaves(RedisClusterNode master) {
    return doInScope(CLUSTER_SLAVES, () -> connection.clusterGetSlaves(master));
  }

  @Override
  public Map<RedisClusterNode, Collection<RedisClusterNode>> clusterGetMasterSlaveMap() {
    return doInScope(CLUSTER_MASTER_SLAVE_MAP, () -> connection.clusterGetMasterSlaveMap());
  }

  @Override
  public Integer clusterGetSlotForKey(byte[] key) {
    return doInScope(CLUSTER_KEYSLOT, () -> connection.clusterGetSlotForKey(key));
  }

  @Override
  public RedisClusterNode clusterGetNodeForSlot(int slot) {
    return doInScope(CLUSTER_NODE_FOR_SLOT, () -> connection.clusterGetNodeForSlot(slot));
  }

  @Override
  public RedisClusterNode clusterGetNodeForKey(byte[] key) {
    return doInScope(CLUSTER_NODE_FOR_KEY, () -> connection.clusterGetNodeForKey(key));
  }

  @Override
  public ClusterInfo clusterGetClusterInfo() {
    return doInScope(CLUSTER_INFO, () -> connection.clusterGetClusterInfo());
  }

  @Override
  public void clusterAddSlots(RedisClusterNode node, int... slots) {
    doInScope(CLUSTER_ADDSLOTS, () -> connection.clusterAddSlots(node, slots));
  }

  @Override
  public void clusterAddSlots(RedisClusterNode node, SlotRange range) {
    doInScope(CLUSTER_ADDSLOTS, () -> connection.clusterAddSlots(node, range));
  }

  @Override
  public Long clusterCountKeysInSlot(int slot) {
    return doInScope(CLUSTER_COUNTKEYSINSLOT, () -> connection.clusterCountKeysInSlot(slot));
  }

  @Override
  public void clusterDeleteSlots(RedisClusterNode node, int... slots) {
    doInScope(CLUSTER_DELSLOTS, () -> connection.clusterDeleteSlots(node, slots));
  }

  @Override
  public void clusterDeleteSlotsInRange(RedisClusterNode node, SlotRange range) {
    doInScope(CLUSTER_DELSLOTS, () -> connection.clusterDeleteSlotsInRange(node, range));
  }

  @Override
  public void clusterForget(RedisClusterNode node) {
    doInScope(CLUSTER_FORGET, () -> connection.clusterForget(node));
  }

  @Override
  public void clusterMeet(RedisClusterNode node) {
    doInScope(CLUSTER_MEET, () -> connection.clusterMeet(node));
  }

  @Override
  public void clusterSetSlot(RedisClusterNode node, int slot, AddSlots mode) {
    doInScope(CLUSTER_SETSLOT, () -> connection.clusterSetSlot(node, slot, mode));
  }

  @Override
  public List<byte[]> clusterGetKeysInSlot(int slot, Integer count) {
    return doInScope(CLUSTER_GETKEYSINSLOT, () -> connection.clusterGetKeysInSlot(slot, count));
  }

  @Override
  public void clusterReplicate(RedisClusterNode master, RedisClusterNode slave) {
    doInScope(CLUSTER_REPLICATE, () -> connection.clusterReplicate(master, slave));
  }

  @Override
  public String ping(RedisClusterNode node) {
    return doInScope(PING, () -> connection.ping(node));
  }

  @Override
  public void bgReWriteAof(RedisClusterNode node) {
    doInScope(BGREWRITEAOF, () -> connection.bgReWriteAof(node));
  }

  @Override
  public void bgSave(RedisClusterNode node) {
    doInScope(BGSAVE, () -> connection.bgSave(node));
  }

  @Override
  public Long lastSave(RedisClusterNode node) {
    return doInScope(LASTSAVE, () -> connection.lastSave(node));
  }

  @Override
  public void save(RedisClusterNode node) {
    doInScope(SAVE, () -> connection.save(node));
  }

  @Override
  public Long dbSize(RedisClusterNode node) {
    return doInScope(DBSIZE, () -> connection.dbSize(node));
  }

  @Override
  public void flushDb(RedisClusterNode node) {
    doInScope(FLUSHDB, () -> connection.flushDb(node));
  }

  @Override
  public void flushAll(RedisClusterNode node) {
    doInScope(FLUSHALL, () -> connection.flushAll(node));
  }

  @Override
  public Properties info(RedisClusterNode node) {
    return doInScope(INFO, () -> connection.info(node));
  }

  @Override
  public Properties info(RedisClusterNode node, String section) {
    return doInScope(INFO, () -> connection.info(node, section));
  }

  @Override
  public Set<byte[]> keys(RedisClusterNode node, byte[] pattern) {
    return doInScope(KEYS, () -> connection.keys(node, pattern));
  }

  @Override
  public byte[] randomKey(RedisClusterNode node) {
    return doInScope(RANDOMKEY, () -> connection.randomKey(node));
  }

  @Override
  public void shutdown(RedisClusterNode node) {
    doInScope(SHUTDOWN, () -> connection.shutdown(node));
  }

  @Override
  public Properties getConfig(RedisClusterNode node, String pattern) {
    return doInScope(CONFIG_GET, () -> connection.getConfig(node, pattern));
  }

  @Override
  public void setConfig(RedisClusterNode node, String param, String value) {
    doInScope(CONFIG_SET, () -> connection.setConfig(node, param, value));
  }

  @Override
  public void resetConfigStats(RedisClusterNode node) {
    doInScope(CONFIG_RESETSTAT, () -> connection.resetConfigStats(node));
  }

  @Override
  public Long time(RedisClusterNode node) {
    return doInScope(TIME, () -> connection.time(node));
  }

  @Override
  public List<RedisClientInfo> getClientList(RedisClusterNode node) {
    return doInScope(CLIENT_LIST, () -> connection.getClientList(node));
  }

}

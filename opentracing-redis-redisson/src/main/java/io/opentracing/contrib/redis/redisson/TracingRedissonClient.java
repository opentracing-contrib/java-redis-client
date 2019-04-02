/*
 * Copyright 2017-2019 The OpenTracing Authors
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
package io.opentracing.contrib.redis.redisson;

import io.opentracing.Tracer;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import java.util.concurrent.TimeUnit;
import org.redisson.api.BatchOptions;
import org.redisson.api.ClusterNodesGroup;
import org.redisson.api.ExecutorOptions;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.MapOptions;
import org.redisson.api.Node;
import org.redisson.api.NodesGroup;
import org.redisson.api.RAtomicDouble;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBatch;
import org.redisson.api.RBinaryStream;
import org.redisson.api.RBitSet;
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RBoundedBlockingQueue;
import org.redisson.api.RBucket;
import org.redisson.api.RBuckets;
import org.redisson.api.RCountDownLatch;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RDeque;
import org.redisson.api.RDoubleAdder;
import org.redisson.api.RGeo;
import org.redisson.api.RHyperLogLog;
import org.redisson.api.RKeys;
import org.redisson.api.RLexSortedSet;
import org.redisson.api.RList;
import org.redisson.api.RListMultimap;
import org.redisson.api.RListMultimapCache;
import org.redisson.api.RLiveObjectService;
import org.redisson.api.RLocalCachedMap;
import org.redisson.api.RLock;
import org.redisson.api.RLongAdder;
import org.redisson.api.RMap;
import org.redisson.api.RMapCache;
import org.redisson.api.RPatternTopic;
import org.redisson.api.RPermitExpirableSemaphore;
import org.redisson.api.RPriorityBlockingDeque;
import org.redisson.api.RPriorityBlockingQueue;
import org.redisson.api.RPriorityDeque;
import org.redisson.api.RPriorityQueue;
import org.redisson.api.RQueue;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RRemoteService;
import org.redisson.api.RScheduledExecutorService;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RSemaphore;
import org.redisson.api.RSet;
import org.redisson.api.RSetCache;
import org.redisson.api.RSetMultimap;
import org.redisson.api.RSetMultimapCache;
import org.redisson.api.RSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.RTopic;
import org.redisson.api.RTransaction;
import org.redisson.api.RedissonClient;
import org.redisson.api.TransactionOptions;
import org.redisson.client.codec.Codec;
import org.redisson.config.Config;

public class TracingRedissonClient implements RedissonClient {
  private final RedissonClient redissonClient;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRedissonClient(RedissonClient redissonClient, Tracer tracer,
      boolean traceWithActiveSpanOnly) {
    this.redissonClient = redissonClient;
    this.tracingRedissonHelper = new TracingRedissonHelper(
        new TracingConfiguration.Builder(tracer).traceWithActiveSpanOnly(traceWithActiveSpanOnly)
            .build());
  }

  @Override
  public <K, V> RStream<K, V> getStream(String name) {
    return redissonClient.getStream(name);
  }

  @Override
  public <K, V> RStream<K, V> getStream(String name,
      Codec codec) {
    return redissonClient.getStream(name, codec);
  }

  @Override
  public RRateLimiter getRateLimiter(String name) {
    return redissonClient.getRateLimiter(name);
  }

  @Override
  public RBinaryStream getBinaryStream(String name) {
    return redissonClient.getBinaryStream(name);
  }

  @Override
  public <V> RGeo<V> getGeo(String name) {
    return redissonClient.getGeo(name);
  }

  @Override
  public <V> RGeo<V> getGeo(String name, Codec codec) {
    return redissonClient.getGeo(name, codec);
  }

  @Override
  public <V> RSetCache<V> getSetCache(String name) {
    return new TracingRSetCache<>(redissonClient.getSetCache(name), tracingRedissonHelper);
  }

  @Override
  public <V> RSetCache<V> getSetCache(String name, Codec codec) {
    return new TracingRSetCache<>(redissonClient.getSetCache(name, codec), tracingRedissonHelper);
  }

  @Override
  public <K, V> RMapCache<K, V> getMapCache(String name, Codec codec) {
    return new TracingRMapCache<>(redissonClient.getMapCache(name, codec), tracingRedissonHelper);
  }

  @Override
  public <K, V> RMapCache<K, V> getMapCache(String name, Codec codec, MapOptions<K, V> options) {
    return new TracingRMapCache<>(redissonClient.getMapCache(name, codec, options),
        tracingRedissonHelper);
  }

  @Override
  public <K, V> RMapCache<K, V> getMapCache(String name) {
    return new TracingRMapCache<>(redissonClient.getMapCache(name), tracingRedissonHelper);
  }

  @Override
  public <K, V> RMapCache<K, V> getMapCache(String name, MapOptions<K, V> options) {
    return new TracingRMapCache<>(redissonClient.getMapCache(name, options), tracingRedissonHelper);
  }

  @Override
  public <V> RBucket<V> getBucket(String name) {
    return new TracingRBucket<>(redissonClient.getBucket(name), tracingRedissonHelper);
  }

  @Override
  public <V> RBucket<V> getBucket(String name, Codec codec) {
    return new TracingRBucket<>(redissonClient.getBucket(name, codec), tracingRedissonHelper);
  }

  @Override
  public RBuckets getBuckets() {
    return new TracingRBuckets(redissonClient.getBuckets(), tracingRedissonHelper);
  }

  @Override
  public RBuckets getBuckets(Codec codec) {
    return new TracingRBuckets(redissonClient.getBuckets(codec), tracingRedissonHelper);
  }

  @Override
  public <V> RHyperLogLog<V> getHyperLogLog(String name) {
    return redissonClient.getHyperLogLog(name);
  }

  @Override
  public <V> RHyperLogLog<V> getHyperLogLog(String name,
      Codec codec) {
    return redissonClient.getHyperLogLog(name, codec);
  }

  @Override
  public <V> RList<V> getList(String name) {
    return new TracingRList<>(redissonClient.getList(name), tracingRedissonHelper);
  }

  @Override
  public <V> RList<V> getList(String name, Codec codec) {
    return new TracingRList<>(redissonClient.getList(name, codec), tracingRedissonHelper);
  }

  @Override
  public <K, V> RListMultimap<K, V> getListMultimap(String name) {
    return new TracingRListMultimap<>(redissonClient.getListMultimap(name), tracingRedissonHelper);
  }

  @Override
  public <K, V> RListMultimap<K, V> getListMultimap(String name,
      Codec codec) {
    return new TracingRListMultimap<>(redissonClient.getListMultimap(name, codec),
        tracingRedissonHelper);
  }

  @Override
  public <K, V> RListMultimapCache<K, V> getListMultimapCache(String name) {
    return redissonClient.getListMultimapCache(name);
  }

  @Override
  public <K, V> RListMultimapCache<K, V> getListMultimapCache(String name,
      Codec codec) {
    return redissonClient.getListMultimapCache(name, codec);
  }

  @Override
  public <K, V> RLocalCachedMap<K, V> getLocalCachedMap(String name,
      LocalCachedMapOptions<K, V> options) {
    return new TracingRLocalCachedMap<>(redissonClient.getLocalCachedMap(name, options),
        tracingRedissonHelper);
  }

  @Override
  public <K, V> RLocalCachedMap<K, V> getLocalCachedMap(String name,
      Codec codec, LocalCachedMapOptions<K, V> options) {
    return new TracingRLocalCachedMap<>(redissonClient.getLocalCachedMap(name, codec, options),
        tracingRedissonHelper);
  }

  @Override
  public <K, V> RMap<K, V> getMap(String name) {
    return new TracingRMap<>(redissonClient.getMap(name), tracingRedissonHelper);
  }

  @Override
  public <K, V> RMap<K, V> getMap(String name,
      MapOptions<K, V> options) {
    return new TracingRMap<>(redissonClient.getMap(name, options), tracingRedissonHelper);
  }

  @Override
  public <K, V> RMap<K, V> getMap(String name, Codec codec) {
    return new TracingRMap<>(redissonClient.getMap(name, codec), tracingRedissonHelper);
  }

  @Override
  public <K, V> RMap<K, V> getMap(String name, Codec codec,
      MapOptions<K, V> options) {
    return new TracingRMap<>(redissonClient.getMap(name, codec, options), tracingRedissonHelper);
  }

  @Override
  public <K, V> RSetMultimap<K, V> getSetMultimap(String name) {
    return new TracingRSetMultimap<>(redissonClient.getSetMultimap(name), tracingRedissonHelper);
  }

  @Override
  public <K, V> RSetMultimap<K, V> getSetMultimap(String name,
      Codec codec) {
    return new TracingRSetMultimap<>(redissonClient.getSetMultimap(name, codec),
        tracingRedissonHelper);
  }

  @Override
  public <K, V> RSetMultimapCache<K, V> getSetMultimapCache(String name) {
    return new TracingRSetMultimapCache<>(redissonClient.getSetMultimapCache(name),
        tracingRedissonHelper);
  }

  @Override
  public <K, V> RSetMultimapCache<K, V> getSetMultimapCache(String name,
      Codec codec) {
    return new TracingRSetMultimapCache<>(redissonClient.getSetMultimapCache(name, codec),
        tracingRedissonHelper);
  }

  @Override
  public RSemaphore getSemaphore(String name) {
    return new TracingRSemaphore(redissonClient.getSemaphore(name), tracingRedissonHelper);
  }

  @Override
  public RPermitExpirableSemaphore getPermitExpirableSemaphore(String name) {
    return new TracingRPermitExpirableSemaphore(redissonClient.getPermitExpirableSemaphore(name),
        tracingRedissonHelper);
  }

  @Override
  public RLock getLock(String name) {
    return new TracingRLock(redissonClient.getLock(name), tracingRedissonHelper);
  }

  @Override
  public RLock getFairLock(String name) {
    return new TracingRLock(redissonClient.getFairLock(name), tracingRedissonHelper);
  }

  @Override
  public RReadWriteLock getReadWriteLock(String name) {
    return new TracingRReadWriteLock(redissonClient.getReadWriteLock(name), tracingRedissonHelper);
  }

  @Override
  public <V> RSet<V> getSet(String name) {
    return new TracingRSet<>(redissonClient.getSet(name), tracingRedissonHelper);
  }

  @Override
  public <V> RSet<V> getSet(String name, Codec codec) {
    return new TracingRSet<>(redissonClient.getSet(name, codec), tracingRedissonHelper);
  }

  @Override
  public <V> RSortedSet<V> getSortedSet(String name) {
    return new TracingRSortedSet<>(redissonClient.getSortedSet(name), tracingRedissonHelper);
  }

  @Override
  public <V> RSortedSet<V> getSortedSet(String name,
      Codec codec) {
    return new TracingRSortedSet<>(redissonClient.getSortedSet(name, codec), tracingRedissonHelper);
  }

  @Override
  public <V> RScoredSortedSet<V> getScoredSortedSet(String name) {
    return new TracingRScoredSortedSet<>(redissonClient.getScoredSortedSet(name),
        tracingRedissonHelper);
  }

  @Override
  public <V> RScoredSortedSet<V> getScoredSortedSet(String name, Codec codec) {
    return new TracingRScoredSortedSet<>(redissonClient.getScoredSortedSet(name, codec),
        tracingRedissonHelper);
  }

  @Override
  public RLexSortedSet getLexSortedSet(String name) {
    return new TracingRLexSortedSet(redissonClient.getLexSortedSet(name), tracingRedissonHelper);
  }

  @Override
  public RTopic getTopic(String name) {
    return redissonClient.getTopic(name);
  }

  @Override
  public RTopic getTopic(String name, Codec codec) {
    return redissonClient.getTopic(name, codec);
  }

  @Override
  public RPatternTopic getPatternTopic(String pattern) {
    return redissonClient.getPatternTopic(pattern);
  }

  @Override
  public RPatternTopic getPatternTopic(String pattern,
      Codec codec) {
    return redissonClient.getPatternTopic(pattern, codec);
  }

  @Override
  public <V> RQueue<V> getQueue(String name) {
    return new TracingRQueue<>(redissonClient.getQueue(name), tracingRedissonHelper);
  }

  @Override
  public <V> RDelayedQueue<V> getDelayedQueue(RQueue<V> destinationQueue) {
    return new TracingRDelayedQueue<>(redissonClient.getDelayedQueue(destinationQueue),
        tracingRedissonHelper);
  }

  @Override
  public <V> RQueue<V> getQueue(String name, Codec codec) {
    return new TracingRQueue<>(redissonClient.getQueue(name, codec), tracingRedissonHelper);
  }

  @Override
  public <V> RPriorityQueue<V> getPriorityQueue(String name) {
    return new TracingRPriorityQueue<>(redissonClient.getPriorityQueue(name),
        tracingRedissonHelper);
  }

  @Override
  public <V> RPriorityQueue<V> getPriorityQueue(String name,
      Codec codec) {
    return new TracingRPriorityQueue<>(redissonClient.getPriorityQueue(name, codec),
        tracingRedissonHelper);
  }

  @Override
  public <V> RPriorityBlockingQueue<V> getPriorityBlockingQueue(String name) {
    return new TracingRPriorityBlockingQueue<>(redissonClient.getPriorityBlockingQueue(name),
        tracingRedissonHelper);
  }

  @Override
  public <V> RPriorityBlockingQueue<V> getPriorityBlockingQueue(String name,
      Codec codec) {
    return new TracingRPriorityBlockingQueue<>(redissonClient.getPriorityBlockingQueue(name, codec),
        tracingRedissonHelper);
  }

  @Override
  public <V> RPriorityBlockingDeque<V> getPriorityBlockingDeque(String name) {
    return new TracingRPriorityBlockingDeque<>(redissonClient.getPriorityBlockingDeque(name),
        tracingRedissonHelper);
  }

  @Override
  public <V> RPriorityBlockingDeque<V> getPriorityBlockingDeque(String name, Codec codec) {
    return new TracingRPriorityBlockingDeque<>(redissonClient.getPriorityBlockingDeque(name, codec),
        tracingRedissonHelper);
  }

  @Override
  public <V> RPriorityDeque<V> getPriorityDeque(String name) {
    return new TracingRPriorityDeque<>(redissonClient.getPriorityDeque(name),
        tracingRedissonHelper);
  }

  @Override
  public <V> RPriorityDeque<V> getPriorityDeque(String name, Codec codec) {
    return new TracingRPriorityDeque<>(redissonClient.getPriorityDeque(name, codec),
        tracingRedissonHelper);
  }

  @Override
  public <V> RBlockingQueue<V> getBlockingQueue(String name) {
    return new TracingRBlockingQueue<>(redissonClient.getBlockingQueue(name),
        tracingRedissonHelper);
  }

  @Override
  public <V> RBlockingQueue<V> getBlockingQueue(String name,
      Codec codec) {
    return new TracingRBlockingQueue<>(redissonClient.getBlockingQueue(name, codec),
        tracingRedissonHelper);
  }

  @Override
  public <V> RBoundedBlockingQueue<V> getBoundedBlockingQueue(String name) {
    return new TracingRBoundedBlockingQueue<>(redissonClient.getBoundedBlockingQueue(name),
        tracingRedissonHelper);
  }

  @Override
  public <V> RBoundedBlockingQueue<V> getBoundedBlockingQueue(String name, Codec codec) {
    return new TracingRBoundedBlockingQueue<>(redissonClient.getBoundedBlockingQueue(name, codec),
        tracingRedissonHelper);
  }

  @Override
  public <V> RDeque<V> getDeque(String name) {
    return new TracingRDeque<>(redissonClient.getDeque(name), tracingRedissonHelper);
  }

  @Override
  public <V> RDeque<V> getDeque(String name, Codec codec) {
    return new TracingRDeque<>(redissonClient.getDeque(name, codec), tracingRedissonHelper);
  }

  @Override
  public <V> RBlockingDeque<V> getBlockingDeque(String name) {
    return new TracingRBlockingDeque<>(redissonClient.getBlockingDeque(name),
        tracingRedissonHelper);
  }

  @Override
  public <V> RBlockingDeque<V> getBlockingDeque(String name, Codec codec) {
    return new TracingRBlockingDeque<>(redissonClient.getBlockingDeque(name, codec),
        tracingRedissonHelper);
  }

  @Override
  public RAtomicLong getAtomicLong(String name) {
    return new TracingRAtomicLong(redissonClient.getAtomicLong(name), tracingRedissonHelper);
  }

  @Override
  public RAtomicDouble getAtomicDouble(String name) {
    return new TracingRAtomicDouble(redissonClient.getAtomicDouble(name),
        tracingRedissonHelper);
  }

  @Override
  public RLongAdder getLongAdder(String name) {
    return new TracingRLongAdder(redissonClient.getLongAdder(name), tracingRedissonHelper);
  }

  @Override
  public RDoubleAdder getDoubleAdder(String name) {
    return new TracingRDoubleAdder(redissonClient.getDoubleAdder(name), tracingRedissonHelper);
  }

  @Override
  public RCountDownLatch getCountDownLatch(String name) {
    return new TracingRCountDownLatch(redissonClient.getCountDownLatch(name),
        tracingRedissonHelper);
  }

  @Override
  public RBitSet getBitSet(String name) {
    return new TracingRBitSet(redissonClient.getBitSet(name),
        tracingRedissonHelper);
  }

  @Override
  public <V> RBloomFilter<V> getBloomFilter(String name) {
    return redissonClient.getBloomFilter(name);
  }

  @Override
  public <V> RBloomFilter<V> getBloomFilter(String name,
      Codec codec) {
    return redissonClient.getBloomFilter(name, codec);
  }

  @Override
  public RScript getScript() {
    return redissonClient.getScript();
  }

  @Override
  public RScript getScript(Codec codec) {
    return redissonClient.getScript(codec);
  }

  @Override
  public RScheduledExecutorService getExecutorService(String name) {
    return redissonClient.getExecutorService(name);
  }

  @Override
  public RScheduledExecutorService getExecutorService(String name,
      ExecutorOptions options) {
    return redissonClient.getExecutorService(name, options);
  }

  @Override
  @Deprecated
  public RScheduledExecutorService getExecutorService(
      Codec codec, String name) {
    return redissonClient.getExecutorService(codec, name);
  }

  @Override
  public RScheduledExecutorService getExecutorService(String name,
      Codec codec) {
    return redissonClient.getExecutorService(name, codec);
  }

  @Override
  public RScheduledExecutorService getExecutorService(String name,
      Codec codec, ExecutorOptions options) {
    return redissonClient.getExecutorService(name, codec, options);
  }

  @Override
  public RRemoteService getRemoteService() {
    return redissonClient.getRemoteService();
  }

  @Override
  public RRemoteService getRemoteService(Codec codec) {
    return redissonClient.getRemoteService(codec);
  }

  @Override
  public RRemoteService getRemoteService(String name) {
    return redissonClient.getRemoteService(name);
  }

  @Override
  public RRemoteService getRemoteService(String name,
      Codec codec) {
    return redissonClient.getRemoteService(name, codec);
  }

  @Override
  public RTransaction createTransaction(TransactionOptions options) {
    return redissonClient.createTransaction(options);
  }

  @Override
  public RBatch createBatch(BatchOptions options) {
    return redissonClient.createBatch(options);
  }

  @Override
  @Deprecated
  public RBatch createBatch() {
    return redissonClient.createBatch();
  }

  @Override
  public RKeys getKeys() {
    return new TracingRKeys(redissonClient.getKeys(), tracingRedissonHelper);
  }

  @Override
  public RLiveObjectService getLiveObjectService() {
    return redissonClient.getLiveObjectService();
  }

  @Override
  public void shutdown() {
    redissonClient.shutdown();
  }

  @Override
  public void shutdown(long quietPeriod, long timeout, TimeUnit unit) {
    redissonClient.shutdown(quietPeriod, timeout, unit);
  }

  @Override
  public Config getConfig() {
    return redissonClient.getConfig();
  }

  @Override
  public NodesGroup<Node> getNodesGroup() {
    return redissonClient.getNodesGroup();
  }

  @Override
  public ClusterNodesGroup getClusterNodesGroup() {
    return redissonClient.getClusterNodesGroup();
  }

  @Override
  public boolean isShutdown() {
    return redissonClient.isShutdown();
  }

  @Override
  public boolean isShuttingDown() {
    return redissonClient.isShuttingDown();
  }
}

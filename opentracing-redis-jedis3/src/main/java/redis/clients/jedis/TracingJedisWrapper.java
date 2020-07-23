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
package redis.clients.jedis;

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

import io.opentracing.Span;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.contrib.redis.common.TracingHelper;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.params.ClientKillParams;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.MigrateParams;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;
import redis.clients.jedis.util.Slowlog;

/**
 * TracingJedisWrapper wraps Jedis object, usually at the moment of extraction from the Pool. Used
 * to provide tracing capabilities to redis commands executed by the client provided by given Jedis
 * object.
 */
public class TracingJedisWrapper extends Jedis {

  private final TracingHelper helper;
  private final Jedis wrapped;

  public TracingJedisWrapper(TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis();
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(final String host, TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(host);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(final String host, final int port,
      TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(host, port);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(final String host, final int port, final boolean ssl,
      TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(host, port, ssl);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(final String host, final int port, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(final String host, final int port, final int timeout,
      TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(host, port, timeout);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(final String host, final int port, final int timeout,
      final boolean ssl,
      TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(host, port, timeout, ssl);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(final String host, final int port, final int timeout,
      final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(host, port, timeout, ssl, sslSocketFactory, sslParameters,
        hostnameVerifier);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(final String host, final int port, final int connectionTimeout,
      final int soTimeout, TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(host, port, connectionTimeout, soTimeout);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(final String host, final int port, final int connectionTimeout,
      final int soTimeout,
      final boolean ssl, TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(host, port, connectionTimeout, soTimeout, ssl);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(final String host, final int port, final int connectionTimeout,
      final int soTimeout,
      final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    this.wrapped = new TracingJedisWrapper(
        new Jedis(host, port, connectionTimeout, soTimeout, ssl, sslSocketFactory, sslParameters,
            hostnameVerifier), tracingConfiguration);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(JedisShardInfo shardInfo, TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(shardInfo);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(URI uri, TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(uri);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(URI uri, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(uri, sslSocketFactory, sslParameters, hostnameVerifier);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(final URI uri, final int timeout,
      TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(uri, timeout);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(final URI uri, final int timeout,
      final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(final URI uri, final int connectionTimeout, final int soTimeout,
      TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(uri, connectionTimeout, soTimeout);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(final URI uri, final int connectionTimeout, final int soTimeout,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    this.wrapped = new Jedis(uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters,
        hostnameVerifier);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisWrapper(Jedis jedis, TracingConfiguration tracingConfiguration) {
    this.wrapped = jedis;
    this.helper = new TracingHelper(tracingConfiguration);
  }

  @Override
  public Tuple zpopmax(String key) {
    Span span = helper.buildSpan("zpopmax");
    span.setTag("key", key);
    return helper.decorate(span, () -> wrapped.zpopmax(key));
  }

  @Override
  public Set<Tuple> zpopmax(String key, int count) {
    Span span = helper.buildSpan("zpopmax");
    span.setTag("key", key);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zpopmax(key, count));
  }

  @Override
  public Tuple zpopmin(String key) {
    Span span = helper.buildSpan("zpopmin");
    span.setTag("key", key);
    return helper.decorate(span, () -> wrapped.zpopmin(key));
  }

  @Override
  public Set<Tuple> zpopmin(String key, int count) {
    Span span = helper.buildSpan("zpopmin");
    span.setTag("key", key);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zpopmin(key, count));
  }

  @Override
  public String memoryDoctor() {
    Span span = helper.buildSpan("memoryDoctor");
    return helper.decorate(span, () -> wrapped.memoryDoctor());
  }

  @Override
  public StreamEntryID xadd(String key, StreamEntryID id,
      Map<String, String> hash) {
    Span span = helper.buildSpan("xadd");
    span.setTag("key", key);
    span.setTag("id", nullable(id));
    return helper.decorate(span, () -> wrapped.xadd(key, id, hash));
  }

  @Override
  public StreamEntryID xadd(String key, StreamEntryID id,
      Map<String, String> hash, long maxLen, boolean approximateLength) {
    Span span = helper.buildSpan("xadd");
    span.setTag("key", key);
    span.setTag("id", nullable(id));
    span.setTag("maxLen", maxLen);
    span.setTag("approximateLength", approximateLength);
    return helper.decorate(span, () -> wrapped.xadd(key, id, hash, maxLen, approximateLength));
  }

  @Override
  public Long xlen(String key) {
    Span span = helper.buildSpan("xlen");
    span.setTag("key", key);
    return helper.decorate(span, () -> wrapped.xlen(key));
  }

  @Override
  public List<StreamEntry> xrange(String key, StreamEntryID start,
      StreamEntryID end, int count) {
    Span span = helper.buildSpan("xrange");
    span.setTag("key", key);
    span.setTag("start", nullable(start));
    span.setTag("end", nullable(end));
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.xrange(key, start, end, count));
  }

  @Override
  public List<StreamEntry> xrevrange(String key, StreamEntryID end,
      StreamEntryID start, int count) {
    Span span = helper.buildSpan("xrevrange");
    span.setTag("key", key);
    span.setTag("start", nullable(start));
    span.setTag("end", nullable(end));
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.xrevrange(key, start, end, count));
  }

  @Override
  public List<Entry<String, List<StreamEntry>>> xread(int count, long block,
      Entry<String, StreamEntryID>... streams) {
    Span span = helper.buildSpan("xread");
    span.setTag("count", count);
    span.setTag("block", block);
    return helper.decorate(span, () -> wrapped.xread(count, block, streams));
  }

  @Override
  public long xack(String key, String group, StreamEntryID... ids) {
    Span span = helper.buildSpan("xack");
    span.setTag("key", key);
    span.setTag("group", group);
    span.setTag("ids", Arrays.toString(ids));
    return helper.decorate(span, () -> wrapped.xack(key, group, ids));
  }

  @Override
  public String xgroupCreate(String key, String groupname, StreamEntryID id, boolean makeStream) {
    Span span = helper.buildSpan("xgroupCreate");
    span.setTag("key", key);
    span.setTag("groupname", groupname);
    span.setTag("id", nullable(id));
    span.setTag("makeStream", makeStream);
    return helper.decorate(span, () -> wrapped.xgroupCreate(key, groupname, id, makeStream));
  }

  @Override
  public String xgroupSetID(String key, String groupname, StreamEntryID id) {
    Span span = helper.buildSpan("xgroupSetID");
    span.setTag("key", key);
    span.setTag("groupname", groupname);
    span.setTag("id", nullable(id));
    return helper.decorate(span, () -> wrapped.xgroupSetID(key, groupname, id));
  }

  @Override
  public long xgroupDestroy(String key, String groupname) {
    Span span = helper.buildSpan("xgroupDestroy");
    span.setTag("key", key);
    span.setTag("groupname", groupname);
    return helper.decorate(span, () -> wrapped.xgroupDestroy(key, groupname));
  }

  @Override
  public Long xgroupDelConsumer(String key, String groupname, String consumerName) {
    Span span = helper.buildSpan("xgroupDelConsumer");
    span.setTag("key", key);
    span.setTag("groupname", groupname);
    span.setTag("consumerName", consumerName);
    return helper.decorate(span, () -> wrapped.xgroupDelConsumer(key, groupname, consumerName));
  }

  @Override
  public long xdel(String key, StreamEntryID... ids) {
    Span span = helper.buildSpan("xdel");
    span.setTag("key", key);
    span.setTag("ids", Arrays.toString(ids));
    return helper.decorate(span, () -> wrapped.xdel(key, ids));
  }

  @Override
  public long xtrim(String key, long maxLen, boolean approximateLength) {
    Span span = helper.buildSpan("xtrim");
    span.setTag("key", key);
    span.setTag("maxLen", maxLen);
    span.setTag("approximateLength", approximateLength);
    return helper.decorate(span, () -> wrapped.xtrim(key, maxLen, approximateLength));
  }

  @Override
  public List<Entry<String, List<StreamEntry>>> xreadGroup(String groupname, String consumer,
      int count, long block, boolean noAck,
      Entry<String, StreamEntryID>... streams) {
    Span span = helper.buildSpan("xreadGroup");
    span.setTag("groupname", groupname);
    span.setTag("consumer", consumer);
    span.setTag("count", count);
    span.setTag("block", block);
    span.setTag("noAck", noAck);
    return helper
        .decorate(span, () -> wrapped.xreadGroup(groupname, consumer, count, block, noAck));
  }

  @Override
  public List<StreamPendingEntry> xpending(String key, String groupname,
      StreamEntryID start, StreamEntryID end, int count, String consumername) {
    Span span = helper.buildSpan("xpending");
    span.setTag("key", key);
    span.setTag("groupname", groupname);
    span.setTag("start", nullable(start));
    span.setTag("end", nullable(end));
    span.setTag("count", count);
    span.setTag("consumername", consumername);
    return helper
        .decorate(span, () -> wrapped.xpending(key, groupname, start, end, count, consumername));
  }

  @Override
  public List<StreamEntry> xclaim(String key, String group, String consumername, long minIdleTime,
      long newIdleTime, int retries, boolean force, StreamEntryID... ids) {
    Span span = helper.buildSpan("xclaim");
    span.setTag("key", key);
    span.setTag("group", group);
    span.setTag("consumername", consumername);
    span.setTag("minIdleTime", minIdleTime);
    span.setTag("retries", retries);
    span.setTag("force", force);
    span.setTag("ids", Arrays.toString(ids));
    return helper
        .decorate(span, () -> wrapped
            .xclaim(key, group, consumername, minIdleTime, newIdleTime, retries, force, ids));
  }

  @Override
  public StreamInfo xinfoStream(String key) {
    Span span = helper.buildSpan("xinfoStream");
    span.setTag("key", key);
    return helper.decorate(span, () -> wrapped.xinfoStream(key));
  }

  @Override
  public StreamInfo xinfoStream(byte[] key) {
    Span span = helper.buildSpan("xinfoStream");
    span.setTag("key", Arrays.toString(key));
    return helper.decorate(span, () -> wrapped.xinfoStream(key));
  }

  @Override
  public List<StreamGroupInfo> xinfoGroup(String key) {
    Span span = helper.buildSpan("xinfoGroup");
    span.setTag("key", key);
    return helper.decorate(span, () -> wrapped.xinfoGroup(key));
  }

  @Override
  public List<StreamGroupInfo> xinfoGroup(byte[] key) {
    Span span = helper.buildSpan("xinfoGroup");
    span.setTag("key", Arrays.toString(key));
    return helper.decorate(span, () -> wrapped.xinfoGroup(key));
  }

  @Override
  public List<StreamConsumersInfo> xinfoConsumers(String key, String group) {
    Span span = helper.buildSpan("xinfoConsumers");
    span.setTag("key", key);
    span.setTag("group", group);
    return helper.decorate(span, () -> wrapped.xinfoConsumers(key, group));
  }

  @Override
  public List<StreamConsumersInfo> xinfoConsumers(byte[] key, byte[] group) {
    Span span = helper.buildSpan("xinfoConsumers");
    span.setTag("key", Arrays.toString(key));
    span.setTag("group", Arrays.toString(group));
    return helper.decorate(span, () -> wrapped.xinfoConsumers(key, group));
  }

  @Override
  public List<Long> bitfieldReadonly(String key, String... arguments) {
    Span span = helper.buildSpan("bitfieldReadonly");
    span.setTag("key", key);
    span.setTag("arguments", Arrays.toString(arguments));
    return helper.decorate(span, () -> wrapped.bitfieldReadonly(key, arguments));
  }

  @Override
  public List<Long> bitfieldReadonly(byte[] key, byte[]... arguments) {
    Span span = helper.buildSpan("bitfieldReadonly");
    span.setTag("key", Arrays.toString(key));
    span.setTag("arguments", Arrays.toString(arguments));
    return helper.decorate(span, () -> wrapped.bitfieldReadonly(key, arguments));
  }

  @Override
  public List<String> objectHelp() {
    Span span = helper.buildSpan("objectHelp");
    return helper.decorate(span, () -> wrapped.objectHelp());
  }

  @Override
  public List<byte[]> objectHelpBinary() {
    Span span = helper.buildSpan("objectHelpBinary");
    return helper.decorate(span, () -> wrapped.objectHelpBinary());
  }

  @Override
  public Long objectFreq(String key) {
    Span span = helper.buildSpan("objectFreq");
    span.setTag("key", key);
    return helper.decorate(span, () -> wrapped.objectFreq(key));
  }

  @Override
  public Long objectFreq(byte[] key) {
    Span span = helper.buildSpan("objectFreq");
    span.setTag("key", Arrays.toString(key));
    return helper.decorate(span, () -> wrapped.objectFreq(key));
  }

  @Override
  public Object sendCommand(ProtocolCommand cmd,
      String... args) {
    Span span = helper.buildSpan("sendCommand");
    span.setTag("cmd", nullable(cmd));
    span.setTag("args", Arrays.toString(args));
    return helper.decorate(span, () -> wrapped.sendCommand(cmd, args));
  }

  @Override
  public Tuple zpopmax(byte[] key) {
    Span span = helper.buildSpan("zpopmax");
    span.setTag("key", Arrays.toString(key));
    return helper.decorate(span, () -> wrapped.zpopmax(key));
  }

  @Override
  public Set<Tuple> zpopmax(byte[] key, int count) {
    Span span = helper.buildSpan("zpopmax");
    span.setTag("key", Arrays.toString(key));
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zpopmax(key, count));
  }

  @Override
  public Tuple zpopmin(byte[] key) {
    Span span = helper.buildSpan("zpopmin");
    span.setTag("key", Arrays.toString(key));
    return helper.decorate(span, () -> wrapped.zpopmin(key));
  }

  @Override
  public Set<Tuple> zpopmin(byte[] key, int count) {
    Span span = helper.buildSpan("zpopmin");
    span.setTag("key", Arrays.toString(key));
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zpopmin(key, count));
  }

  @Override
  public byte[] memoryDoctorBinary() {
    Span span = helper.buildSpan("memoryDoctorBinary");
    return helper.decorate(span, () -> wrapped.memoryDoctorBinary());
  }

  @Override
  public List<byte[]> xread(int count, long block, Map<byte[], byte[]> streams) {
    Span span = helper.buildSpan("xread");
    span.setTag("count", count);
    span.setTag("block", block);
    return helper.decorate(span, () -> wrapped.xread(count, block, streams));
  }

  @Override
  public List<byte[]> xreadGroup(byte[] groupname, byte[] consumer, int count, long block,
      boolean noAck, Map<byte[], byte[]> streams) {
    Span span = helper.buildSpan("xreadGroup");
    span.setTag("groupname", Arrays.toString(groupname));
    span.setTag("consumer", Arrays.toString(consumer));
    span.setTag("count", count);
    span.setTag("block", block);
    span.setTag("noAck", noAck);
    return helper.decorate(span,
        () -> wrapped.xreadGroup(groupname, consumer, count, block, noAck, streams));
  }

  @Override
  public byte[] xadd(byte[] key, byte[] id, Map<byte[], byte[]> hash, long maxLen,
      boolean approximateLength) {
    Span span = helper.buildSpan("xadd");
    span.setTag("key", Arrays.toString(key));
    span.setTag("id", Arrays.toString(id));
    span.setTag("maxLen", maxLen);
    span.setTag("approximateLength", approximateLength);
    return helper.decorate(span, () -> wrapped.xadd(key, id, hash, maxLen, approximateLength));
  }

  @Override
  public Long xlen(byte[] key) {
    Span span = helper.buildSpan("xlen");
    span.setTag("key", Arrays.toString(key));
    return helper.decorate(span, () -> wrapped.xlen(key));
  }

  @Override
  public List<byte[]> xrange(byte[] key, byte[] start, byte[] end, long count) {
    Span span = helper.buildSpan("xrange");
    span.setTag("key", Arrays.toString(key));
    span.setTag("start", Arrays.toString(start));
    span.setTag("end", Arrays.toString(end));
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.xrange(key, start, end, count));
  }

  @Override
  public List<byte[]> xrevrange(byte[] key, byte[] end, byte[] start, int count) {
    Span span = helper.buildSpan("xrevrange");
    span.setTag("key", Arrays.toString(key));
    span.setTag("end", Arrays.toString(end));
    span.setTag("start", Arrays.toString(start));
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.xrevrange(key, end, start, count));
  }

  @Override
  public Long xack(byte[] key, byte[] group, byte[]... ids) {
    Span span = helper.buildSpan("xack");
    span.setTag("key", Arrays.toString(key));
    span.setTag("group", Arrays.toString(group));
    span.setTag("ids", TracingHelper.toString(ids));
    return helper.decorate(span, () -> wrapped.xack(key, group, ids));
  }

  @Override
  public String xgroupCreate(byte[] key, byte[] consumer, byte[] id, boolean makeStream) {
    Span span = helper.buildSpan("xgroupCreate");
    span.setTag("key", Arrays.toString(key));
    span.setTag("consumer", Arrays.toString(consumer));
    span.setTag("id", Arrays.toString(id));
    span.setTag("makeStream", makeStream);
    return helper.decorate(span, () -> wrapped.xgroupCreate(key, consumer, id, makeStream));
  }

  @Override
  public String xgroupSetID(byte[] key, byte[] consumer, byte[] id) {
    Span span = helper.buildSpan("xgroupSetID");
    span.setTag("key", Arrays.toString(key));
    span.setTag("consumer", Arrays.toString(consumer));
    span.setTag("id", Arrays.toString(id));
    return helper.decorate(span, () -> wrapped.xgroupSetID(key, consumer, id));
  }

  @Override
  public Long xgroupDestroy(byte[] key, byte[] consumer) {
    Span span = helper.buildSpan("xgroupDestroy");
    span.setTag("key", Arrays.toString(key));
    span.setTag("consumer", Arrays.toString(consumer));
    return helper.decorate(span, () -> wrapped.xgroupDestroy(key, consumer));
  }

  @Override
  public Long xgroupDelConsumer(byte[] key, byte[] consumer, byte[] consumerName) {
    Span span = helper.buildSpan("xgroupDelConsumer");
    span.setTag("key", Arrays.toString(key));
    span.setTag("consumer", Arrays.toString(consumer));
    span.setTag("consumerName", Arrays.toString(consumerName));
    return helper.decorate(span, () -> wrapped.xgroupDelConsumer(key, consumer, consumerName));
  }

  @Override
  public Long xdel(byte[] key, byte[]... ids) {
    Span span = helper.buildSpan("xdel");
    span.setTag("key", Arrays.toString(key));
    span.setTag("ids", TracingHelper.toString(ids));
    return helper.decorate(span, () -> wrapped.xdel(key, ids));
  }

  @Override
  public Long xtrim(byte[] key, long maxLen, boolean approximateLength) {
    Span span = helper.buildSpan("xtrim");
    span.setTag("key", Arrays.toString(key));
    span.setTag("maxLen", maxLen);
    span.setTag("approximateLength", approximateLength);
    return helper.decorate(span, () -> wrapped.xtrim(key, maxLen, approximateLength));
  }

  @Override
  public List<byte[]> xpending(byte[] key, byte[] groupname, byte[] start, byte[] end, int count,
      byte[] consumername) {
    Span span = helper.buildSpan("xpending");
    span.setTag("key", Arrays.toString(key));
    span.setTag("groupname", Arrays.toString(groupname));
    span.setTag("start", Arrays.toString(start));
    span.setTag("end", Arrays.toString(end));
    span.setTag("count", count);
    span.setTag("consumername", Arrays.toString(consumername));
    return helper
        .decorate(span, () -> wrapped.xpending(key, groupname, start, end, count, consumername));
  }

  @Override
  public List<byte[]> xclaim(byte[] key, byte[] groupname, byte[] consumername, long minIdleTime,
      long newIdleTime, int retries, boolean force, byte[][] ids) {
    Span span = helper.buildSpan("xclaim");
    span.setTag("key", Arrays.toString(key));
    span.setTag("groupname", Arrays.toString(groupname));
    span.setTag("consumername", Arrays.toString(consumername));
    span.setTag("minIdleTime", minIdleTime);
    span.setTag("newIdleTime", newIdleTime);
    span.setTag("retries", retries);
    span.setTag("force", force);
    span.setTag("ids", TracingHelper.toString(ids));
    return helper
        .decorate(span, () -> wrapped
            .xclaim(key, groupname, consumername, minIdleTime, newIdleTime, retries, force, ids));
  }

  @Override
  public Object sendCommand(ProtocolCommand cmd, byte[]... args) {
    Span span = helper.buildSpan("sendCommand");
    span.setTag("cmd", nullable(cmd));
    span.setTag("args", TracingHelper.toString(args));
    return helper.decorate(span, () -> wrapped.sendCommand(cmd, args));
  }

  @Override
  public Object sendCommand(ProtocolCommand cmd) {
    Span span = helper.buildSpan("sendCommand");
    span.setTag("cmd", nullable(cmd));
    return helper.decorate(span, () -> wrapped.sendCommand(cmd));
  }

  @Override
  public String ping(String message) {
    Span span = helper.buildSpan("ping");
    span.setTag("message", message);
    return helper.decorate(span, () -> wrapped.ping(message));
  }

  @Override
  public String set(String key, String value) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.set(key, value));
  }

  @Override
  public String set(String key, String value, SetParams params) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", value);
    span.setTag("params", nullable(TracingHelper.toString(params.getByteParams())));
    return helper.decorate(span, () -> wrapped.set(key, value, params));
  }

  @Override
  public String get(String key) {
    Span span = helper.buildSpan("get", key);
    return helper.decorate(span, () -> wrapped.get(key));
  }

  @Override
  public Long exists(String... keys) {
    Span span = helper.buildSpan("exists", keys);
    return helper.decorate(span, () -> wrapped.exists(keys));
  }

  @Override
  public Boolean exists(String key) {
    Span span = helper.buildSpan("exists", key);
    return helper.decorate(span, () -> wrapped.exists(key));
  }

  @Override
  public Long del(String... keys) {
    Span span = helper.buildSpan("del", keys);
    return helper.decorate(span, () -> wrapped.del(keys));
  }

  @Override
  public Long del(String key) {
    Span span = helper.buildSpan("del", key);
    return helper.decorate(span, () -> wrapped.del(key));
  }

  @Override
  public Long unlink(String... keys) {
    Span span = helper.buildSpan("unlink", keys);
    return helper.decorate(span, () -> wrapped.unlink(keys));
  }

  @Override
  public Long unlink(String key) {
    Span span = helper.buildSpan("unlink", key);
    return helper.decorate(span, () -> wrapped.unlink(key));
  }

  @Override
  public String type(String key) {
    Span span = helper.buildSpan("type", key);
    return helper.decorate(span, () -> wrapped.type(key));
  }

  @Override
  public Set<String> keys(String pattern) {
    Span span = helper.buildSpan("keys");
    span.setTag("pattern", nullable(pattern));
    return helper.decorate(span, () -> wrapped.keys(pattern));
  }

  @Override
  public String randomKey() {
    Span span = helper.buildSpan("randomKey");
    return helper.decorate(span, () -> wrapped.randomKey());
  }

  @Override
  public String rename(String oldkey, String newkey) {
    Span span = helper.buildSpan("rename");
    span.setTag("oldKey", nullable(oldkey));
    span.setTag("newKey", nullable(newkey));
    return helper.decorate(span, () -> wrapped.rename(oldkey, newkey));
  }

  @Override
  public Long renamenx(String oldkey, String newkey) {
    Span span = helper.buildSpan("renamenx");
    span.setTag("oldKey", nullable(oldkey));
    span.setTag("newKey", nullable(newkey));
    return helper.decorate(span, () -> wrapped.renamenx(oldkey, newkey));
  }

  @Override
  public Long expire(String key, int seconds) {
    Span span = helper.buildSpan("expire", key);
    span.setTag("seconds", seconds);
    return helper.decorate(span, () -> wrapped.expire(key, seconds));
  }

  @Override
  public Long expireAt(String key, long unixTime) {
    Span span = helper.buildSpan("expireAt", key);
    span.setTag("unixTime", unixTime);
    return helper.decorate(span, () -> wrapped.expireAt(key, unixTime));
  }

  @Override
  public Long ttl(String key) {
    Span span = helper.buildSpan("ttl", key);
    return helper.decorate(span, () -> wrapped.ttl(key));
  }

  @Override
  public Long touch(String... keys) {
    Span span = helper.buildSpan("touch", keys);
    return helper.decorate(span, () -> wrapped.touch(keys));
  }

  @Override
  public Long touch(String key) {
    Span span = helper.buildSpan("touch", key);
    return helper.decorate(span, () -> wrapped.touch(key));
  }

  @Override
  public Long move(String key, int dbIndex) {
    Span span = helper.buildSpan("move", key);
    span.setTag("dbIndex", dbIndex);
    return helper.decorate(span, () -> wrapped.move(key, dbIndex));
  }

  @Override
  public String getSet(String key, String value) {
    Span span = helper.buildSpan("getSet", key);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.getSet(key, value));
  }

  @Override
  public List<String> mget(String... keys) {
    Span span = helper.buildSpan("mget", keys);
    return helper.decorate(span, () -> wrapped.mget(keys));
  }

  @Override
  public Long setnx(String key, String value) {
    Span span = helper.buildSpan("setnx", key);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.setnx(key, value));
  }

  @Override
  public String setex(String key, int seconds, String value) {
    Span span = helper.buildSpan("setex", key);
    span.setTag("seconds", seconds);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.setex(key, seconds, value));
  }

  @Override
  public String mset(String... keysvalues) {
    Span span = helper.buildSpan("mset");
    span.setTag("keysvalues", Arrays.toString(keysvalues));
    return helper.decorate(span, () -> wrapped.mset(keysvalues));
  }

  @Override
  public Long msetnx(String... keysvalues) {
    Span span = helper.buildSpan("msetnx");
    span.setTag("keysvalues", Arrays.toString(keysvalues));
    return helper.decorate(span, () -> wrapped.msetnx(keysvalues));
  }

  @Override
  public Long decrBy(String key, long integer) {
    Span span = helper.buildSpan("decrBy", key);
    span.setTag("integer", integer);
    return helper.decorate(span, () -> wrapped.decrBy(key, integer));
  }

  @Override
  public Long decr(String key) {
    Span span = helper.buildSpan("decr", key);
    return helper.decorate(span, () -> wrapped.decr(key));
  }

  @Override
  public Long incrBy(String key, long integer) {
    Span span = helper.buildSpan("incrBy", key);
    span.setTag("integer", integer);
    return helper.decorate(span, () -> wrapped.incrBy(key, integer));
  }

  @Override
  public Double incrByFloat(String key, double value) {
    Span span = helper.buildSpan("incrByFloat", key);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.incrByFloat(key, value));
  }

  @Override
  public Long incr(String key) {
    Span span = helper.buildSpan("incr", key);
    return helper.decorate(span, () -> wrapped.incr(key));
  }

  @Override
  public Long append(String key, String value) {
    Span span = helper.buildSpan("append", key);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.append(key, value));
  }

  @Override
  public String substr(String key, int start, int end) {
    Span span = helper.buildSpan("substr", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.substr(key, start, end));
  }

  @Override
  public Long hset(String key, String field, String value) {
    Span span = helper.buildSpan("hset", key);
    span.setTag("field", field);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.hset(key, field, value));
  }

  @Override
  public Long hset(String key, Map<String, String> hash) {
    Span span = helper.buildSpan("hset", key);
    span.setTag("hash", TracingHelper.toString(hash));
    return helper.decorate(span, () -> wrapped.hset(key, hash));
  }

  @Override
  public String hget(String key, String field) {
    Span span = helper.buildSpan("hget", key);
    span.setTag("field", field);
    return helper.decorate(span, () -> wrapped.hget(key, field));
  }

  @Override
  public Long hsetnx(String key, String field, String value) {
    Span span = helper.buildSpan("hsetnx", key);
    span.setTag("field", field);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.hsetnx(key, field, value));
  }

  @Override
  public String hmset(String key, Map<String, String> hash) {
    Span span = helper.buildSpan("hmset", key);
    span.setTag("hash", TracingHelper.toString(hash));
    return helper.decorate(span, () -> wrapped.hmset(key, hash));
  }

  @Override
  public List<String> hmget(String key, String... fields) {
    Span span = helper.buildSpan("hmget", key);
    span.setTag("fields", Arrays.toString(fields));
    return helper.decorate(span, () -> wrapped.hmget(key, fields));
  }

  @Override
  public Long hincrBy(String key, String field, long value) {
    Span span = helper.buildSpan("hincrBy", key);
    span.setTag("field", field);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.hincrBy(key, field, value));
  }

  @Override
  public Double hincrByFloat(String key, String field, double value) {
    Span span = helper.buildSpan("hincrByFloat", key);
    span.setTag("field", field);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.hincrByFloat(key, field, value));
  }

  @Override
  public Boolean hexists(String key, String field) {
    Span span = helper.buildSpan("hexists", key);
    span.setTag("field", field);
    return helper.decorate(span, () -> wrapped.hexists(key, field));
  }

  @Override
  public Long hdel(String key, String... fields) {
    Span span = helper.buildSpan("hdel", key);
    span.setTag("fields", Arrays.toString(fields));
    return helper.decorate(span, () -> wrapped.hdel(key, fields));
  }

  @Override
  public Long hlen(String key) {
    Span span = helper.buildSpan("hlen", key);
    return helper.decorate(span, () -> wrapped.hlen(key));
  }

  @Override
  public Set<String> hkeys(String key) {
    Span span = helper.buildSpan("hkeys", key);
    return helper.decorate(span, () -> wrapped.hkeys(key));
  }

  @Override
  public List<String> hvals(String key) {
    Span span = helper.buildSpan("hvals", key);
    return helper.decorate(span, () -> wrapped.hvals(key));
  }

  @Override
  public Map<String, String> hgetAll(String key) {
    Span span = helper.buildSpan("hgetAll", key);
    return helper.decorate(span, () -> wrapped.hgetAll(key));
  }

  @Override
  public Long rpush(String key, String... strings) {
    Span span = helper.buildSpan("rpush", key);
    span.setTag("strings", Arrays.toString(strings));
    return helper.decorate(span, () -> wrapped.rpush(key, strings));
  }

  @Override
  public Long lpush(String key, String... strings) {
    Span span = helper.buildSpan("lpush", key);
    span.setTag("strings", Arrays.toString(strings));
    return helper.decorate(span, () -> wrapped.lpush(key, strings));
  }

  @Override
  public Long llen(String key) {
    Span span = helper.buildSpan("llen", key);
    return helper.decorate(span, () -> wrapped.llen(key));
  }

  @Override
  public List<String> lrange(String key, long start, long end) {
    Span span = helper.buildSpan("lrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.lrange(key, start, end));
  }

  @Override
  public String ltrim(String key, long start, long end) {
    Span span = helper.buildSpan("ltrim", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.ltrim(key, start, end));
  }

  @Override
  public String lindex(String key, long index) {
    Span span = helper.buildSpan("lindex", key);
    span.setTag("index", index);
    return helper.decorate(span, () -> wrapped.lindex(key, index));
  }

  @Override
  public String lset(String key, long index, String value) {
    Span span = helper.buildSpan("lset", key);
    span.setTag("index", index);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.lset(key, index, value));
  }

  @Override
  public Long lrem(String key, long count, String value) {
    Span span = helper.buildSpan("lrem", key);
    span.setTag("count", count);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.lrem(key, count, value));
  }

  @Override
  public String lpop(String key) {
    Span span = helper.buildSpan("lpop", key);
    return helper.decorate(span, () -> wrapped.lpop(key));
  }

  @Override
  public String rpop(String key) {
    Span span = helper.buildSpan("rpop", key);
    return helper.decorate(span, () -> wrapped.rpop(key));
  }

  @Override
  public String rpoplpush(String srckey, String dstkey) {
    Span span = helper.buildSpan("rpoplpush");
    span.setTag("srckey", srckey);
    span.setTag("dstkey", dstkey);
    return helper.decorate(span, () -> wrapped.rpoplpush(srckey, dstkey));
  }

  @Override
  public Long sadd(String key, String... members) {
    Span span = helper.buildSpan("sadd", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> wrapped.sadd(key, members));
  }

  @Override
  public Set<String> smembers(String key) {
    Span span = helper.buildSpan("smembers", key);
    return helper.decorate(span, () -> wrapped.smembers(key));
  }

  @Override
  public Long srem(String key, String... members) {
    Span span = helper.buildSpan("srem", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> wrapped.srem(key, members));
  }

  @Override
  public String spop(String key) {
    Span span = helper.buildSpan("spop", key);
    return helper.decorate(span, () -> wrapped.spop(key));
  }

  @Override
  public Set<String> spop(String key, long count) {
    Span span = helper.buildSpan("spop", key);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.spop(key, count));
  }

  @Override
  public Long smove(String srckey, String dstkey, String member) {
    Span span = helper.buildSpan("smove");
    span.setTag("srckey", srckey);
    span.setTag("dstkey", dstkey);
    span.setTag("member", member);
    return helper.decorate(span, () -> wrapped.smove(srckey, dstkey, member));
  }

  @Override
  public Long scard(String key) {
    Span span = helper.buildSpan("scard", key);
    return helper.decorate(span, () -> wrapped.scard(key));
  }

  @Override
  public Boolean sismember(String key, String member) {
    Span span = helper.buildSpan("sismember", key);
    span.setTag("member", member);
    return helper.decorate(span, () -> wrapped.sismember(key, member));
  }

  @Override
  public Set<String> sinter(String... keys) {
    Span span = helper.buildSpan("sinter", keys);
    return helper.decorate(span, () -> wrapped.sinter(keys));
  }

  @Override
  public Long sinterstore(String dstkey, String... keys) {
    Span span = helper.buildSpan("sinterstore", keys);
    span.setTag("dstkey", dstkey);
    return helper.decorate(span, () -> wrapped.sinterstore(dstkey, keys));
  }

  @Override
  public Set<String> sunion(String... keys) {
    Span span = helper.buildSpan("sunion", keys);
    return helper.decorate(span, () -> wrapped.sunion(keys));
  }

  @Override
  public Long sunionstore(String dstkey, String... keys) {
    Span span = helper.buildSpan("sunionstore", keys);
    span.setTag("dstkey", dstkey);
    return helper.decorate(span, () -> wrapped.sunionstore(dstkey, keys));
  }

  @Override
  public Set<String> sdiff(String... keys) {
    Span span = helper.buildSpan("sdiff", keys);
    return helper.decorate(span, () -> wrapped.sdiff(keys));
  }

  @Override
  public Long sdiffstore(String dstkey, String... keys) {
    Span span = helper.buildSpan("sdiffstore", keys);
    span.setTag("dstkey", dstkey);
    return helper.decorate(span, () -> wrapped.sdiffstore(dstkey, keys));
  }

  @Override
  public String srandmember(String key) {
    Span span = helper.buildSpan("srandmember", key);
    return helper.decorate(span, () -> wrapped.srandmember(key));
  }

  @Override
  public List<String> srandmember(String key, int count) {
    Span span = helper.buildSpan("srandmember", key);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.srandmember(key, count));
  }

  @Override
  public Long zadd(String key, double score, String member) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("score", score);
    span.setTag("member", member);
    return helper.decorate(span, () -> wrapped.zadd(key, score, member));
  }

  @Override
  public Long zadd(String key, double score, String member, ZAddParams params) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("score", score);
    span.setTag("member", member);
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper.decorate(span, () -> wrapped.zadd(key, score, member, params));
  }

  @Override
  public Long zadd(String key, Map<String, Double> scoreMembers) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("scoreMembers", TracingHelper.toString(scoreMembers));
    return helper.decorate(span, () -> wrapped.zadd(key, scoreMembers));
  }

  @Override
  public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("scoreMembers", TracingHelper.toString(scoreMembers));
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper.decorate(span, () -> wrapped.zadd(key, scoreMembers, params));
  }

  @Override
  public Set<String> zrange(String key, long start, long end) {
    Span span = helper.buildSpan("zrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.zrange(key, start, end));
  }

  @Override
  public Long zrem(String key, String... members) {
    Span span = helper.buildSpan("zrem", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> wrapped.zrem(key, members));
  }

  @Override
  public Double zincrby(String key, double score, String member) {
    Span span = helper.buildSpan("zincrby", key);
    span.setTag("score", score);
    span.setTag("member", member);
    return helper.decorate(span, () -> wrapped.zincrby(key, score, member));
  }

  @Override
  public Double zincrby(String key, double increment, String member, ZIncrByParams params) {
    Span span = helper.buildSpan("zincrby", key);
    span.setTag("increment", increment);
    span.setTag("member", member);
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper.decorate(span, () -> wrapped.zincrby(key, increment, member, params));
  }

  @Override
  public Long zrank(String key, String member) {
    Span span = helper.buildSpan("zrank", key);
    span.setTag("member", member);
    return helper.decorate(span, () -> wrapped.zrank(key, member));
  }

  @Override
  public Long zrevrank(String key, String member) {
    Span span = helper.buildSpan("zrevrank", key);
    span.setTag("member", member);
    return helper.decorate(span, () -> wrapped.zrevrank(key, member));
  }

  @Override
  public Set<String> zrevrange(String key, long start, long end) {
    Span span = helper.buildSpan("zrevrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.zrevrange(key, start, end));
  }

  @Override
  public Set<Tuple> zrangeWithScores(String key, long start, long end) {
    Span span = helper.buildSpan("zrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.zrangeWithScores(key, start, end));
  }

  @Override
  public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
    Span span = helper.buildSpan("zrevrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.zrevrangeWithScores(key, start, end));
  }

  @Override
  public Long zcard(String key) {
    Span span = helper.buildSpan("zcard", key);
    return helper.decorate(span, () -> wrapped.zcard(key));
  }

  @Override
  public Double zscore(String key, String member) {
    Span span = helper.buildSpan("zscore", key);
    span.setTag("member", member);
    return helper.decorate(span, () -> wrapped.zscore(key, member));
  }

  @Override
  public String watch(String... keys) {
    Span span = helper.buildSpan("watch", keys);
    return helper.decorate(span, () -> wrapped.watch(keys));
  }

  @Override
  public List<String> sort(String key) {
    Span span = helper.buildSpan("sort", key);
    return helper.decorate(span, () -> wrapped.sort(key));
  }

  @Override
  public List<String> sort(String key, SortingParams sortingParameters) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("sortingParameters", TracingHelper.toString(sortingParameters.getParams()));
    return helper.decorate(span, () -> wrapped.sort(key, sortingParameters));
  }

  @Override
  public List<String> blpop(int timeout, String... keys) {
    Span span = helper.buildSpan("blpop", keys);
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> wrapped.blpop(timeout, keys));
  }

  @Override
  public List<String> blpop(String... args) {
    Span span = helper.buildSpan("blpop");
    span.setTag("args", Arrays.toString(args));
    return helper.decorate(span, () -> wrapped.blpop(args));
  }

  @Override
  public List<String> brpop(String... args) {
    Span span = helper.buildSpan("brpop");
    span.setTag("args", Arrays.toString(args));
    return helper.decorate(span, () -> wrapped.brpop(args));
  }

  @Override
  public Long sort(String key, SortingParams sortingParameters, String dstkey) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("sortingParameters", TracingHelper.toString(sortingParameters.getParams()));
    span.setTag("dstkey", dstkey);
    return helper.decorate(span, () -> wrapped.sort(key, sortingParameters, dstkey));
  }

  @Override
  public Long sort(String key, String dstkey) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("dstkey", dstkey);
    return helper.decorate(span, () -> wrapped.sort(key, dstkey));
  }

  @Override
  public List<String> brpop(int timeout, String... keys) {
    Span span = helper.buildSpan("brpop", keys);
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> wrapped.brpop(timeout, keys));
  }

  @Override
  public Long zcount(String key, double min, double max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> wrapped.zcount(key, min, max));
  }

  @Override
  public Long zcount(String key, String min, String max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> wrapped.zcount(key, min, max));
  }

  @Override
  public Set<String> zrangeByScore(String key, double min, double max) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> wrapped.zrangeByScore(key, min, max));
  }

  @Override
  public Set<String> zrangeByScore(String key, String min, String max) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> wrapped.zrangeByScore(key, min, max));
  }

  @Override
  public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zrangeByScore(key, min, max, offset, count));
  }

  @Override
  public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zrangeByScore(key, min, max, offset, count));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> wrapped.zrangeByScoreWithScores(key, min, max));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> wrapped.zrangeByScoreWithScores(key, min, max));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset,
      int count) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper
        .decorate(span, () -> wrapped.zrangeByScoreWithScores(key, min, max, offset, count));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset,
      int count) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper
        .decorate(span, () -> wrapped.zrangeByScoreWithScores(key, min, max, offset, count));
  }

  @Override
  public Set<String> zrevrangeByScore(String key, double max, double min) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return helper.decorate(span, () -> wrapped.zrevrangeByScore(key, max, min));
  }

  @Override
  public Set<String> zrevrangeByScore(String key, String max, String min) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return helper.decorate(span, () -> wrapped.zrevrangeByScore(key, max, min));
  }

  @Override
  public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zrevrangeByScore(key, max, min, offset, count));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return helper.decorate(span, () -> wrapped.zrevrangeByScoreWithScores(key, max, min));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset,
      int count) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper
        .decorate(span, () -> wrapped.zrevrangeByScoreWithScores(key, max, min, offset, count));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset,
      int count) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper
        .decorate(span, () -> wrapped.zrevrangeByScoreWithScores(key, max, min, offset, count));
  }

  @Override
  public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zrevrangeByScore(key, max, min, offset, count));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return helper.decorate(span, () -> wrapped.zrevrangeByScoreWithScores(key, max, min));
  }

  @Override
  public Long zremrangeByRank(String key, long start, long end) {
    Span span = helper.buildSpan("zremrangeByRank", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.zremrangeByRank(key, start, end));
  }

  @Override
  public Long zremrangeByScore(String key, double start, double end) {
    Span span = helper.buildSpan("zremrangeByScore", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.zremrangeByScore(key, start, end));
  }

  @Override
  public Long zremrangeByScore(String key, String start, String end) {
    Span span = helper.buildSpan("zremrangeByScore", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.zremrangeByScore(key, start, end));
  }

  @Override
  public Long zunionstore(String dstkey, String... sets) {
    Span span = helper.buildSpan("zunionstore");
    span.setTag("dstkey", dstkey);
    span.setTag("sets", Arrays.toString(sets));
    return helper.decorate(span, () -> wrapped.zunionstore(dstkey, sets));
  }

  @Override
  public Long zunionstore(String dstkey, ZParams params, String... sets) {
    Span span = helper.buildSpan("zunionstore");
    span.setTag("params", TracingHelper.toString(params.getParams()));
    span.setTag("sets", Arrays.toString(sets));
    return helper.decorate(span, () -> wrapped.zunionstore(dstkey, params, sets));
  }

  @Override
  public Long zinterstore(String dstkey, String... sets) {
    Span span = helper.buildSpan("zinterstore");
    span.setTag("dstkey", dstkey);
    span.setTag("sets", Arrays.toString(sets));
    return helper.decorate(span, () -> wrapped.zinterstore(dstkey, sets));
  }

  @Override
  public Long zinterstore(String dstkey, ZParams params, String... sets) {
    Span span = helper.buildSpan("zinterstore");
    span.setTag("dstkey", dstkey);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    span.setTag("sets", Arrays.toString(sets));
    return helper.decorate(span, () -> wrapped.zinterstore(dstkey, params, sets));
  }

  @Override
  public Long zlexcount(String key, String min, String max) {
    Span span = helper.buildSpan("zlexcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> wrapped.zlexcount(key, min, max));
  }

  @Override
  public Set<String> zrangeByLex(String key, String min, String max) {
    Span span = helper.buildSpan("zrangeByLex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> wrapped.zrangeByLex(key, min, max));
  }

  @Override
  public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByLex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zrangeByLex(key, min, max, offset, count));
  }

  @Override
  public Set<String> zrevrangeByLex(String key, String max, String min) {
    Span span = helper.buildSpan("zrevrangeByLex", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return helper.decorate(span, () -> wrapped.zrevrangeByLex(key, max, min));
  }

  @Override
  public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByLex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zrevrangeByLex(key, max, min, offset, count));
  }

  @Override
  public Long zremrangeByLex(String key, String min, String max) {
    Span span = helper.buildSpan("zremrangeByLex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> wrapped.zremrangeByLex(key, min, max));
  }

  @Override
  public Long strlen(String key) {
    Span span = helper.buildSpan("strlen", key);
    return helper.decorate(span, () -> wrapped.strlen(key));
  }

  @Override
  public Long lpushx(String key, String... string) {
    Span span = helper.buildSpan("lpushx", key);
    span.setTag("string", Arrays.toString(string));
    return helper.decorate(span, () -> wrapped.lpushx(key, string));
  }

  @Override
  public Long persist(String key) {
    Span span = helper.buildSpan("persist", key);
    return helper.decorate(span, () -> wrapped.persist(key));
  }

  @Override
  public Long rpushx(String key, String... string) {
    Span span = helper.buildSpan("rpushx", key);
    span.setTag("string", Arrays.toString(string));
    return helper.decorate(span, () -> wrapped.rpushx(key, string));
  }

  @Override
  public String echo(String string) {
    Span span = helper.buildSpan("echo");
    span.setTag("string", string);
    return helper.decorate(span, () -> wrapped.echo(string));
  }

  @Override
  public Long linsert(String key, ListPosition where, String pivot, String value) {
    Span span = helper.buildSpan("linsert", key);
    span.setTag("where", nullable(where));
    span.setTag("pivot", pivot);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.linsert(key, where, pivot, value));
  }

  @Override
  public String brpoplpush(String source, String destination, int timeout) {
    Span span = helper.buildSpan("brpoplpush");
    span.setTag("source", source);
    span.setTag("destination", destination);
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> wrapped.brpoplpush(source, destination, timeout));
  }

  @Override
  public Boolean setbit(String key, long offset, boolean value) {
    Span span = helper.buildSpan("setbit", key);
    span.setTag("offset", offset);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.setbit(key, offset, value));
  }

  @Override
  public Boolean setbit(String key, long offset, String value) {
    Span span = helper.buildSpan("setbit", key);
    span.setTag("offset", offset);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.setbit(key, offset, value));
  }

  @Override
  public Boolean getbit(String key, long offset) {
    Span span = helper.buildSpan("getbit", key);
    span.setTag("offset", offset);
    return helper.decorate(span, () -> wrapped.getbit(key, offset));
  }

  @Override
  public Long setrange(String key, long offset, String value) {
    Span span = helper.buildSpan("setrange", key);
    span.setTag("offset", offset);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.setrange(key, offset, value));
  }

  @Override
  public String getrange(String key, long startOffset, long endOffset) {
    Span span = helper.buildSpan("getrange", key);
    span.setTag("startOffset", startOffset);
    span.setTag("endOffset", endOffset);
    return helper.decorate(span, () -> wrapped.getrange(key, startOffset, endOffset));
  }

  @Override
  public Long bitpos(String key, boolean value) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.bitpos(key, value));
  }

  @Override
  public Long bitpos(String key, boolean value, BitPosParams params) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("value", value);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> wrapped.bitpos(key, value, params));
  }

  @Override
  public List<String> configGet(String pattern) {
    Span span = helper.buildSpan("configGet");
    span.setTag("pattern", pattern);
    return helper.decorate(span, () -> wrapped.configGet(pattern));
  }

  @Override
  public String configSet(String parameter, String value) {
    Span span = helper.buildSpan("configSet");
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.configSet(parameter, value));
  }

  @Override
  public Object eval(String script, int keyCount, String... params) {
    Span span = helper.buildSpan("eval");
    span.setTag("keyCount", keyCount);
    span.setTag("params", Arrays.toString(params));
    return helper.decorate(span, () -> wrapped.eval(script, keyCount, params));
  }

  @Override
  public void subscribe(JedisPubSub jedisPubSub, String... channels) {
    Span span = helper.buildSpan("subscribe");
    span.setTag("channels", Arrays.toString(channels));
    helper.decorate(span, () -> wrapped.subscribe(jedisPubSub, channels));
  }

  @Override
  public Long publish(String channel, String message) {
    Span span = helper.buildSpan("publish");
    span.setTag("channel", channel);
    span.setTag("message", message);
    return helper.decorate(span, () -> wrapped.publish(channel, message));
  }

  @Override
  public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
    Span span = helper.buildSpan("psubscribe");
    span.setTag("patterns", Arrays.toString(patterns));
    helper.decorate(span, () -> wrapped.psubscribe(jedisPubSub, patterns));
  }

  @Override
  public Object eval(String script, List<String> keys, List<String> args) {
    Span span = helper.buildSpan("eval");
    span.setTag("keys", TracingHelper.toString(keys));
    span.setTag("args", TracingHelper.toString(args));
    return helper.decorate(span, () -> wrapped.eval(script, keys, args));
  }

  @Override
  public Object eval(String script) {
    Span span = helper.buildSpan("eval");
    span.setTag("script", script);
    return helper.decorate(span, () -> wrapped.eval(script));
  }

  @Override
  public Object evalsha(String script) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("script", script);
    return helper.decorate(span, () -> wrapped.evalsha(script));
  }

  @Override
  public Object evalsha(String sha1, List<String> keys, List<String> args) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("keys", TracingHelper.toString(keys));
    span.setTag("args", TracingHelper.toString(args));
    span.setTag("sha1", sha1);
    return helper.decorate(span, () -> wrapped.evalsha(sha1, keys, args));
  }

  @Override
  public Object evalsha(String sha1, int keyCount, String... params) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("keyCount", keyCount);
    span.setTag("params", Arrays.toString(params));
    span.setTag("sha1", sha1);
    return helper.decorate(span, () -> wrapped.evalsha(sha1, keyCount, params));
  }

  @Override
  public Boolean scriptExists(String sha1) {
    Span span = helper.buildSpan("scriptExists");
    span.setTag("sha1", sha1);
    return helper.decorate(span, () -> wrapped.scriptExists(sha1));
  }

  @Override
  public List<Boolean> scriptExists(String... sha1) {
    Span span = helper.buildSpan("scriptExists");
    span.setTag("sha1", Arrays.toString(sha1));
    return helper.decorate(span, () -> wrapped.scriptExists(sha1));
  }

  @Override
  public String scriptLoad(String script) {
    Span span = helper.buildSpan("scriptLoad");
    span.setTag("script", script);
    return helper.decorate(span, () -> wrapped.scriptLoad(script));
  }

  @Override
  public List<Slowlog> slowlogGet() {
    Span span = helper.buildSpan("slowlogGet");
    return helper.decorate(span, () -> wrapped.slowlogGet());
  }

  @Override
  public List<Slowlog> slowlogGet(long entries) {
    Span span = helper.buildSpan("slowlogGet");
    span.setTag("entries", entries);
    return helper.decorate(span, () -> wrapped.slowlogGet(entries));
  }

  @Override
  public Long objectRefcount(String string) {
    Span span = helper.buildSpan("objectRefcount");
    span.setTag("string", string);
    return helper.decorate(span, () -> wrapped.objectRefcount(string));
  }

  @Override
  public String objectEncoding(String string) {
    Span span = helper.buildSpan("objectEncoding");
    span.setTag("string", string);
    return helper.decorate(span, () -> wrapped.objectEncoding(string));
  }

  @Override
  public Long objectIdletime(String string) {
    Span span = helper.buildSpan("objectIdletime");
    span.setTag("string", string);
    return helper.decorate(span, () -> wrapped.objectIdletime(string));
  }

  @Override
  public Long bitcount(String key) {
    Span span = helper.buildSpan("bitcount", key);
    return helper.decorate(span, () -> wrapped.bitcount(key));
  }

  @Override
  public Long bitcount(String key, long start, long end) {
    Span span = helper.buildSpan("bitcount", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.bitcount(key, start, end));
  }

  @Override
  public Long bitop(BitOP op, String destKey, String... srcKeys) {
    Span span = helper.buildSpan("bitop");
    span.setTag("destKey", destKey);
    span.setTag("srcKeys", Arrays.toString(srcKeys));
    return helper.decorate(span, () -> wrapped.bitop(op, destKey, srcKeys));
  }

  @Override
  public List<Map<String, String>> sentinelMasters() {
    Span span = helper.buildSpan("sentinelMasters");
    return helper.decorate(span, () -> wrapped.sentinelMasters());
  }

  @Override
  public List<String> sentinelGetMasterAddrByName(String masterName) {
    Span span = helper.buildSpan("sentinelGetMasterAddrByName");
    span.setTag("masterName", masterName);
    return helper.decorate(span, () -> wrapped.sentinelGetMasterAddrByName(masterName));
  }

  @Override
  public Long sentinelReset(String pattern) {
    Span span = helper.buildSpan("sentinelReset");
    span.setTag("pattern", pattern);
    return helper.decorate(span, () -> wrapped.sentinelReset(pattern));
  }

  @Override
  public List<Map<String, String>> sentinelSlaves(String masterName) {
    Span span = helper.buildSpan("sentinelSlaves");
    span.setTag("masterName", masterName);
    return helper.decorate(span, () -> wrapped.sentinelSlaves(masterName));
  }

  @Override
  public String sentinelFailover(String masterName) {
    Span span = helper.buildSpan("sentinelFailover");
    return helper.decorate(span, () -> wrapped.sentinelFailover(masterName));
  }

  @Override
  public String sentinelMonitor(String masterName, String ip, int port, int quorum) {
    Span span = helper.buildSpan("sentinelMonitor");
    span.setTag("masterName", masterName);
    span.setTag("ip", ip);
    span.setTag("port", port);
    span.setTag("quorum", quorum);
    return helper.decorate(span, () -> wrapped.sentinelMonitor(masterName, ip, port, quorum));
  }

  @Override
  public String sentinelRemove(String masterName) {
    Span span = helper.buildSpan("sentinelRemove");
    span.setTag("masterName", masterName);
    return helper.decorate(span, () -> wrapped.sentinelRemove(masterName));
  }

  @Override
  public String sentinelSet(String masterName, Map<String, String> parameterMap) {
    Span span = helper.buildSpan("sentinelSet");
    span.setTag("masterName", masterName);
    span.setTag("parameterMap", TracingHelper.toString(parameterMap));
    return helper.decorate(span, () -> wrapped.sentinelSet(masterName, parameterMap));
  }

  @Override
  public byte[] dump(String key) {
    Span span = helper.buildSpan("dump", key);
    return helper.decorate(span, () -> wrapped.dump(key));
  }

  @Override
  public String restore(String key, int ttl, byte[] serializedValue) {
    Span span = helper.buildSpan("restore", key);
    span.setTag("ttl", ttl);
    span.setTag("serializedValue", Arrays.toString(serializedValue));
    return helper.decorate(span, () -> wrapped.restore(key, ttl, serializedValue));
  }

  @Override
  public String restoreReplace(String key, int ttl, byte[] serializedValue) {
    Span span = helper.buildSpan("restoreReplace", key);
    span.setTag("ttl", ttl);
    span.setTag("serializedValue", Arrays.toString(serializedValue));
    return helper.decorate(span, () -> wrapped.restoreReplace(key, ttl, serializedValue));
  }

  @Override
  public Long pexpire(String key, long milliseconds) {
    Span span = helper.buildSpan("pexpire", key);
    span.setTag("milliseconds", milliseconds);
    return helper.decorate(span, () -> wrapped.pexpire(key, milliseconds));
  }

  @Override
  public Long pexpireAt(String key, long millisecondsTimestamp) {
    Span span = helper.buildSpan("pexpireAt", key);
    span.setTag("millisecondsTimestamp", millisecondsTimestamp);
    return helper.decorate(span, () -> wrapped.pexpireAt(key, millisecondsTimestamp));
  }

  @Override
  public Long pttl(String key) {
    Span span = helper.buildSpan("pttl", key);
    return helper.decorate(span, () -> wrapped.pttl(key));
  }

  @Override
  public String psetex(String key, long milliseconds, String value) {
    Span span = helper.buildSpan("psetex", key);
    span.setTag("value", value);
    span.setTag("milliseconds", milliseconds);
    return helper.decorate(span, () -> wrapped.psetex(key, milliseconds, value));
  }

  @Override
  public String clientKill(String client) {
    Span span = helper.buildSpan("clientKill");
    span.setTag("client", client);
    return helper.decorate(span, () -> wrapped.clientKill(client));
  }

  @Override
  public String clientGetname() {
    Span span = helper.buildSpan("clientGetname");
    return helper.decorate(span, () -> wrapped.clientGetname());
  }

  @Override
  public String clientList() {
    Span span = helper.buildSpan("clientList");
    return helper.decorate(span, () -> wrapped.clientList());
  }

  @Override
  public String clientSetname(String name) {
    Span span = helper.buildSpan("clientSetname");
    span.setTag("name", name);
    return helper.decorate(span, () -> wrapped.clientSetname(name));
  }

  @Override
  public String migrate(String host, int port, String key, int destinationDb, int timeout) {
    Span span = helper.buildSpan("migrate", key);
    span.setTag("host", host);
    span.setTag("destinationDb", destinationDb);
    span.setTag("timeout", timeout);
    span.setTag("port", port);
    return helper.decorate(span, () -> wrapped.migrate(host, port, key, destinationDb, timeout));
  }

  @Override
  public String migrate(String host, int port, int destinationDB, int timeout, MigrateParams params,
      String... keys) {
    Span span = helper.buildSpan("migrate", keys);
    span.setTag("host", host);
    span.setTag("destinationDB", destinationDB);
    span.setTag("timeout", timeout);
    span.setTag("port", port);
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper
        .decorate(span, () -> wrapped.migrate(host, port, destinationDB, timeout, params, keys));
  }

  @Override
  public ScanResult<String> scan(String cursor) {
    Span span = helper.buildSpan("scan");
    span.setTag("cursor", cursor);
    return helper.decorate(span, () -> wrapped.scan(cursor));
  }

  @Override
  public ScanResult<String> scan(String cursor, ScanParams params) {
    Span span = helper.buildSpan("scan");
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> wrapped.scan(cursor, params));
  }

  @Override
  public ScanResult<Entry<String, String>> hscan(String key, String cursor) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", cursor);
    return helper.decorate(span, () -> wrapped.hscan(key, cursor));
  }

  @Override
  public ScanResult<Entry<String, String>> hscan(String key, String cursor, ScanParams params) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> wrapped.hscan(key, cursor, params));
  }

  @Override
  public ScanResult<String> sscan(String key, String cursor) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", cursor);
    return helper.decorate(span, () -> wrapped.sscan(key, cursor));
  }

  @Override
  public ScanResult<String> sscan(String key, String cursor, ScanParams params) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> wrapped.sscan(key, cursor, params));
  }

  @Override
  public ScanResult<Tuple> zscan(String key, String cursor) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", cursor);
    return helper.decorate(span, () -> wrapped.zscan(key, cursor));
  }

  @Override
  public ScanResult<Tuple> zscan(String key, String cursor, ScanParams params) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> wrapped.zscan(key, cursor, params));
  }

  @Override
  public String clusterNodes() {
    Span span = helper.buildSpan("clusterNodes");
    return helper.decorate(span, () -> wrapped.clusterNodes());
  }

  @Override
  public String readonly() {
    Span span = helper.buildSpan("readonly");
    return helper.decorate(span, () -> wrapped.readonly());
  }

  @Override
  public String clusterMeet(String ip, int port) {
    Span span = helper.buildSpan("clusterMeet");
    span.setTag("ip", ip);
    span.setTag("port", port);
    return helper.decorate(span, () -> wrapped.clusterMeet(ip, port));
  }

  @Override
  public String clusterReset(ClusterReset resetType) {
    Span span = helper.buildSpan("clusterReset");
    span.setTag("resetType", resetType.name());
    return helper.decorate(span, () -> wrapped.clusterReset(resetType));
  }

  @Override
  public String clusterAddSlots(int... slots) {
    Span span = helper.buildSpan("clusterAddSlots");
    span.setTag("slots", Arrays.toString(slots));
    return helper.decorate(span, () -> wrapped.clusterAddSlots(slots));
  }

  @Override
  public String clusterDelSlots(int... slots) {
    Span span = helper.buildSpan("clusterDelSlots");
    span.setTag("slots", Arrays.toString(slots));
    return helper.decorate(span, () -> wrapped.clusterDelSlots(slots));
  }

  @Override
  public String clusterInfo() {
    Span span = helper.buildSpan("clusterInfo");
    return helper.decorate(span, () -> wrapped.clusterInfo());
  }

  @Override
  public List<String> clusterGetKeysInSlot(int slot, int count) {
    Span span = helper.buildSpan("clusterGetKeysInSlot");
    span.setTag("slot", slot);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.clusterGetKeysInSlot(slot, count));
  }

  @Override
  public String clusterSetSlotNode(int slot, String nodeId) {
    Span span = helper.buildSpan("clusterSetSlotNode");
    span.setTag("slot", slot);
    span.setTag("nodeId", nodeId);
    return helper.decorate(span, () -> wrapped.clusterSetSlotNode(slot, nodeId));
  }

  @Override
  public String clusterSetSlotMigrating(int slot, String nodeId) {
    Span span = helper.buildSpan("clusterSetSlotMigrating");
    span.setTag("slot", slot);
    span.setTag("nodeId", nodeId);
    return helper.decorate(span, () -> wrapped.clusterSetSlotMigrating(slot, nodeId));
  }

  @Override
  public String clusterSetSlotImporting(int slot, String nodeId) {
    Span span = helper.buildSpan("clusterSetSlotImporting");
    span.setTag("slot", slot);
    span.setTag("nodeId", nodeId);
    return helper.decorate(span, () -> wrapped.clusterSetSlotImporting(slot, nodeId));
  }

  @Override
  public String clusterSetSlotStable(int slot) {
    Span span = helper.buildSpan("clusterSetSlotStable");
    span.setTag("slot", slot);
    return helper.decorate(span, () -> wrapped.clusterSetSlotStable(slot));
  }

  @Override
  public String clusterForget(String nodeId) {
    Span span = helper.buildSpan("clusterForget");
    span.setTag("nodeId", nodeId);
    return helper.decorate(span, () -> wrapped.clusterForget(nodeId));
  }

  @Override
  public String clusterFlushSlots() {
    Span span = helper.buildSpan("clusterFlushSlots");
    return helper.decorate(span, () -> wrapped.clusterFlushSlots());
  }

  @Override
  public Long clusterKeySlot(String key) {
    Span span = helper.buildSpan("clusterKeySlot", key);
    return helper.decorate(span, () -> wrapped.clusterKeySlot(key));
  }

  @Override
  public Long clusterCountKeysInSlot(int slot) {
    Span span = helper.buildSpan("clusterCountKeysInSlot");
    span.setTag("slot", slot);
    return helper.decorate(span, () -> wrapped.clusterCountKeysInSlot(slot));
  }

  @Override
  public String clusterSaveConfig() {
    Span span = helper.buildSpan("clusterSaveConfig");
    return helper.decorate(span, () -> wrapped.clusterSaveConfig());
  }

  @Override
  public String clusterReplicate(String nodeId) {
    Span span = helper.buildSpan("clusterReplicate");
    span.setTag("nodeId", nodeId);
    return helper.decorate(span, () -> wrapped.clusterReplicate(nodeId));
  }

  @Override
  public List<String> clusterSlaves(String nodeId) {
    Span span = helper.buildSpan("clusterSlaves");
    span.setTag("nodeId", nodeId);
    return helper.decorate(span, () -> wrapped.clusterSlaves(nodeId));
  }

  @Override
  public String clusterFailover() {
    Span span = helper.buildSpan("clusterFailover");
    return helper.decorate(span, () -> wrapped.clusterFailover());
  }

  @Override
  public List<Object> clusterSlots() {
    Span span = helper.buildSpan("clusterSlots");
    return helper.decorate(span, () -> wrapped.clusterSlots());
  }

  @Override
  public String asking() {
    Span span = helper.buildSpan("asking");
    return helper.decorate(span, () -> wrapped.asking());
  }

  @Override
  public List<String> pubsubChannels(String pattern) {
    Span span = helper.buildSpan("pubsubChannels");
    span.setTag("pattern", pattern);
    return helper.decorate(span, () -> wrapped.pubsubChannels(pattern));
  }

  @Override
  public Long pubsubNumPat() {
    Span span = helper.buildSpan("pubsubNumPat");
    return helper.decorate(span, () -> wrapped.pubsubNumPat());
  }

  @Override
  public Map<String, String> pubsubNumSub(String... channels) {
    Span span = helper.buildSpan("pubsubNumSub");
    span.setTag("channels", Arrays.toString(channels));
    return helper.decorate(span, () -> wrapped.pubsubNumSub(channels));
  }

  @Override
  public void close() {
    wrapped.close();
  }

  @Override
  public void setDataSource(JedisPoolAbstract jedisPool) {
    // OT decoration is not needed
    wrapped.setDataSource(jedisPool);
  }

  @Override
  public Long pfadd(String key, String... elements) {
    Span span = helper.buildSpan("pfadd");
    span.setTag("elements", Arrays.toString(elements));
    return helper.decorate(span, () -> wrapped.pfadd(key, elements));
  }

  @Override
  public long pfcount(String key) {
    Span span = helper.buildSpan("pfcount", key);
    return helper.decorate(span, () -> wrapped.pfcount(key));
  }

  @Override
  public long pfcount(String... keys) {
    Span span = helper.buildSpan("pfcount", keys);
    return helper.decorate(span, () -> wrapped.pfcount(keys));
  }

  @Override
  public String pfmerge(String destkey, String... sourcekeys) {
    Span span = helper.buildSpan("pfmerge");
    span.setTag("destkey", destkey);
    span.setTag("sourcekeys", Arrays.toString(sourcekeys));
    return helper.decorate(span, () -> wrapped.pfmerge(destkey, sourcekeys));
  }

  @Override
  public List<String> blpop(int timeout, String key) {
    Span span = helper.buildSpan("blpop");
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> wrapped.blpop(timeout, key));
  }

  @Override
  public List<String> brpop(int timeout, String key) {
    Span span = helper.buildSpan("brpop");
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> wrapped.brpop(timeout, key));
  }

  @Override
  public Long geoadd(String key, double longitude, double latitude, String member) {
    Span span = helper.buildSpan("geoadd");
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("member", member);
    return helper.decorate(span, () -> wrapped.geoadd(key, longitude, latitude, member));
  }

  @Override
  public Long geoadd(String key, Map<String, GeoCoordinate> memberCoordinateMap) {
    Span span = helper.buildSpan("geoadd");
    span.setTag("memberCoordinateMap", TracingHelper.toString(memberCoordinateMap));
    return helper.decorate(span, () -> wrapped.geoadd(key, memberCoordinateMap));
  }

  @Override
  public Double geodist(String key, String member1, String member2) {
    Span span = helper.buildSpan("geodist", key);
    span.setTag("member1", member1);
    span.setTag("member2", member2);
    return helper.decorate(span, () -> wrapped.geodist(key, member1, member2));
  }

  @Override
  public Double geodist(String key, String member1, String member2, GeoUnit unit) {
    Span span = helper.buildSpan("geodist", key);
    span.setTag("member1", member1);
    span.setTag("member2", member2);
    span.setTag("unit", unit.name());
    return helper.decorate(span, () -> wrapped.geodist(key, member1, member2, unit));
  }

  @Override
  public List<String> geohash(String key, String... members) {
    Span span = helper.buildSpan("geohash", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> wrapped.geohash(key, members));
  }

  @Override
  public List<GeoCoordinate> geopos(String key, String... members) {
    Span span = helper.buildSpan("geopos", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> wrapped.geopos(key, members));
  }

  @Override
  public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude,
      double radius, GeoUnit unit) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper.decorate(span, () -> wrapped.georadius(key, longitude, latitude, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude,
      double radius, GeoUnit unit) {
    Span span = helper.buildSpan("georadiusReadonly", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper
        .decorate(span, () -> wrapped.georadiusReadonly(key, longitude, latitude, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude,
      double radius, GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper
        .decorate(span, () -> wrapped.georadius(key, longitude, latitude, radius, unit, param));
  }

  @Override
  public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude,
      double radius, GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadiusReadonly", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper.decorate(span,
        () -> wrapped.georadiusReadonly(key, longitude, latitude, radius, unit, param));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius,
      GeoUnit unit) {
    Span span = helper.buildSpan("georadiusByMember", key);
    span.setTag("member", member);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper.decorate(span, () -> wrapped.georadiusByMember(key, member, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius,
      GeoUnit unit) {
    Span span = helper.buildSpan("georadiusByMemberReadonly", key);
    span.setTag("member", member);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper
        .decorate(span, () -> wrapped.georadiusByMemberReadonly(key, member, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius,
      GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadiusByMember", key);
    span.setTag("member", member);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper.decorate(span, () -> wrapped.georadiusByMember(key, member, radius, unit, param));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius,
      GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadiusByMemberReadonly", key);
    span.setTag("member", member);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper
        .decorate(span, () -> wrapped.georadiusByMemberReadonly(key, member, radius, unit, param));
  }

  @Override
  public String moduleLoad(String path) {
    Span span = helper.buildSpan("moduleLoad");
    span.setTag("path", path);
    return helper.decorate(span, () -> wrapped.moduleLoad(path));
  }

  @Override
  public String moduleUnload(String name) {
    Span span = helper.buildSpan("moduleUnload");
    span.setTag("name", name);
    return helper.decorate(span, () -> wrapped.moduleUnload(name));
  }

  @Override
  public List<Module> moduleList() {
    Span span = helper.buildSpan("moduleList");
    return helper.decorate(span, wrapped::moduleList);
  }

  @Override
  public List<Long> bitfield(String key, String... arguments) {
    Span span = helper.buildSpan("bitfield", key);
    span.setTag("arguments", Arrays.toString(arguments));
    return helper.decorate(span, () -> wrapped.bitfield(key, arguments));
  }

  @Override
  public Long hstrlen(String key, String field) {
    Span span = helper.buildSpan("hstrlen", key);
    span.setTag("field", field);
    return helper.decorate(span, () -> wrapped.hstrlen(key, field));
  }

  @Override
  public String ping() {
    Span span = helper.buildSpan("ping");
    return helper.decorate(span, () -> wrapped.ping());
  }

  @Override
  public byte[] ping(byte[] message) {
    Span span = helper.buildSpan("ping");
    span.setTag("message", Arrays.toString(message));
    return helper.decorate(span, () -> wrapped.ping(message));
  }

  @Override
  public String set(byte[] key, byte[] value) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> wrapped.set(key, value));
  }

  @Override
  public String set(byte[] key, byte[] value, SetParams params) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", Arrays.toString(value));
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper.decorate(span, () -> wrapped.set(key, value, params));
  }

  @Override
  public byte[] get(byte[] key) {
    Span span = helper.buildSpan("get", key);
    return helper.decorate(span, () -> wrapped.get(key));
  }

  @Override
  public String quit() {
    Span span = helper.buildSpan("quit");
    return helper.decorate(span, () -> wrapped.quit());
  }

  @Override
  public Long exists(byte[]... keys) {
    Span span = helper.buildSpan("exists");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> wrapped.exists(keys));
  }

  @Override
  public Boolean exists(byte[] key) {
    Span span = helper.buildSpan("exists", key);
    return helper.decorate(span, () -> wrapped.exists(key));
  }

  @Override
  public Long del(byte[]... keys) {
    Span span = helper.buildSpan("del");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> wrapped.del(keys));
  }

  @Override
  public Long del(byte[] key) {
    Span span = helper.buildSpan("del", key);
    return helper.decorate(span, () -> wrapped.del(key));
  }

  @Override
  public Long unlink(byte[]... keys) {
    Span span = helper.buildSpan("unlink", keys);
    return helper.decorate(span, () -> wrapped.unlink(keys));
  }

  @Override
  public Long unlink(byte[] key) {
    Span span = helper.buildSpan("unlink", key);
    return helper.decorate(span, () -> wrapped.unlink(key));
  }

  @Override
  public String type(byte[] key) {
    Span span = helper.buildSpan("type", key);
    return helper.decorate(span, () -> wrapped.type(key));
  }

  @Override
  public String flushDB() {
    Span span = helper.buildSpan("flushDB");
    return helper.decorate(span, () -> wrapped.flushDB());
  }

  @Override
  public Set<byte[]> keys(byte[] pattern) {
    Span span = helper.buildSpan("keys");
    span.setTag("pattern", Arrays.toString(pattern));
    return helper.decorate(span, () -> wrapped.keys(pattern));
  }

  @Override
  public byte[] randomBinaryKey() {
    Span span = helper.buildSpan("randomBinaryKey");
    return helper.decorate(span, () -> wrapped.randomBinaryKey());
  }

  @Override
  public String rename(byte[] oldkey, byte[] newkey) {
    Span span = helper.buildSpan("rename");
    span.setTag("oldkey", Arrays.toString(oldkey));
    span.setTag("newkey", Arrays.toString(newkey));
    return helper.decorate(span, () -> wrapped.rename(oldkey, newkey));
  }

  @Override
  public Long renamenx(byte[] oldkey, byte[] newkey) {
    Span span = helper.buildSpan("renamenx");
    span.setTag("oldkey", Arrays.toString(oldkey));
    span.setTag("newkey", Arrays.toString(newkey));
    return helper.decorate(span, () -> wrapped.renamenx(oldkey, newkey));
  }

  @Override
  public Long dbSize() {
    Span span = helper.buildSpan("dbSize");
    return helper.decorate(span, () -> wrapped.dbSize());
  }

  @Override
  public Long expire(byte[] key, int seconds) {
    Span span = helper.buildSpan("expire", key);
    span.setTag("seconds", seconds);
    return helper.decorate(span, () -> wrapped.expire(key, seconds));
  }

  @Override
  public Long expireAt(byte[] key, long unixTime) {
    Span span = helper.buildSpan("expireAt", key);
    span.setTag("unixTime", unixTime);
    return helper.decorate(span, () -> wrapped.expireAt(key, unixTime));
  }

  @Override
  public Long ttl(byte[] key) {
    Span span = helper.buildSpan("ttl", key);
    return helper.decorate(span, () -> wrapped.ttl(key));
  }

  @Override
  public Long touch(byte[]... keys) {
    Span span = helper.buildSpan("touch", keys);
    return helper.decorate(span, () -> wrapped.touch(keys));
  }

  @Override
  public Long touch(byte[] key) {
    Span span = helper.buildSpan("touch", key);
    return helper.decorate(span, () -> wrapped.touch(key));
  }

  @Override
  public String select(int index) {
    Span span = helper.buildSpan("select");
    span.setTag("index", index);
    return helper.decorate(span, () -> wrapped.select(index));
  }

  @Override
  public String swapDB(int index1, int index2) {
    Span span = helper.buildSpan("swapDB");
    span.setTag("index1", index1);
    span.setTag("index2", index2);
    return helper.decorate(span, () -> wrapped.swapDB(index1, index2));
  }

  @Override
  public Long move(byte[] key, int dbIndex) {
    Span span = helper.buildSpan("move", key);
    span.setTag("dbIndex", dbIndex);
    return helper.decorate(span, () -> wrapped.move(key, dbIndex));
  }

  @Override
  public String flushAll() {
    Span span = helper.buildSpan("flushAll");
    return helper.decorate(span, () -> wrapped.flushAll());
  }

  @Override
  public byte[] getSet(byte[] key, byte[] value) {
    Span span = helper.buildSpan("getSet", key);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> wrapped.getSet(key, value));
  }

  @Override
  public List<byte[]> mget(byte[]... keys) {
    Span span = helper.buildSpan("mget");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> wrapped.mget(keys));
  }

  @Override
  public Long setnx(byte[] key, byte[] value) {
    Span span = helper.buildSpan("setnx", key);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> wrapped.setnx(key, value));
  }

  @Override
  public String setex(byte[] key, int seconds, byte[] value) {
    Span span = helper.buildSpan("setex", key);
    span.setTag("value", Arrays.toString(value));
    span.setTag("seconds", seconds);
    return helper.decorate(span, () -> wrapped.setex(key, seconds, value));
  }

  @Override
  public String mset(byte[]... keysvalues) {
    Span span = helper.buildSpan("mset");
    span.setTag("keysvalues", TracingHelper.toString(keysvalues));
    return helper.decorate(span, () -> wrapped.mset(keysvalues));
  }

  @Override
  public Long msetnx(byte[]... keysvalues) {
    Span span = helper.buildSpan("msetnx");
    span.setTag("keysvalues", TracingHelper.toString(keysvalues));
    return helper.decorate(span, () -> wrapped.msetnx(keysvalues));
  }

  @Override
  public Long decrBy(byte[] key, long integer) {
    Span span = helper.buildSpan("decrBy", key);
    span.setTag("integer", integer);
    return helper.decorate(span, () -> wrapped.decrBy(key, integer));
  }

  @Override
  public Long decr(byte[] key) {
    Span span = helper.buildSpan("decr", key);
    return helper.decorate(span, () -> wrapped.decr(key));
  }

  @Override
  public Long incrBy(byte[] key, long integer) {
    Span span = helper.buildSpan("incrBy", key);
    span.setTag("integer", integer);
    return helper.decorate(span, () -> wrapped.incrBy(key, integer));
  }

  @Override
  public Double incrByFloat(byte[] key, double integer) {
    Span span = helper.buildSpan("incrByFloat", key);
    span.setTag("integer", integer);
    return helper.decorate(span, () -> wrapped.incrByFloat(key, integer));
  }

  @Override
  public Long incr(byte[] key) {
    Span span = helper.buildSpan("incr", key);
    return helper.decorate(span, () -> wrapped.incr(key));
  }

  @Override
  public Long append(byte[] key, byte[] value) {
    Span span = helper.buildSpan("append", key);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> wrapped.append(key, value));
  }

  @Override
  public byte[] substr(byte[] key, int start, int end) {
    Span span = helper.buildSpan("substr", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.substr(key, start, end));
  }

  @Override
  public Long hset(byte[] key, byte[] field, byte[] value) {
    Span span = helper.buildSpan("hset", key);
    span.setTag("field", Arrays.toString(field));
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> wrapped.hset(key, field, value));
  }

  @Override
  public Long hset(byte[] key, Map<byte[], byte[]> hash) {
    Span span = helper.buildSpan("hset", key);
    span.setTag("hash", TracingHelper.toStringMap(hash));
    return helper.decorate(span, () -> wrapped.hset(key, hash));
  }

  @Override
  public byte[] hget(byte[] key, byte[] field) {
    Span span = helper.buildSpan("hget", key);
    span.setTag("field", Arrays.toString(field));
    return helper.decorate(span, () -> wrapped.hget(key, field));
  }

  @Override
  public Long hsetnx(byte[] key, byte[] field, byte[] value) {
    Span span = helper.buildSpan("hsetnx", key);
    span.setTag("field", Arrays.toString(field));
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> wrapped.hsetnx(key, field, value));
  }

  @Override
  public String hmset(byte[] key, Map<byte[], byte[]> hash) {
    Span span = helper.buildSpan("hmset", key);
    span.setTag("hash", TracingHelper.toStringMap(hash));
    return helper.decorate(span, () -> wrapped.hmset(key, hash));
  }

  @Override
  public List<byte[]> hmget(byte[] key, byte[]... fields) {
    Span span = helper.buildSpan("hmget", key);
    span.setTag("fields", TracingHelper.toString(fields));
    return helper.decorate(span, () -> wrapped.hmget(key, fields));
  }

  @Override
  public Long hincrBy(byte[] key, byte[] field, long value) {
    Span span = helper.buildSpan("hincrBy", key);
    span.setTag("field", Arrays.toString(field));
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.hincrBy(key, field, value));
  }

  @Override
  public Double hincrByFloat(byte[] key, byte[] field, double value) {
    Span span = helper.buildSpan("hincrByFloat", key);
    span.setTag("field", Arrays.toString(field));
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.hincrByFloat(key, field, value));
  }

  @Override
  public Boolean hexists(byte[] key, byte[] field) {
    Span span = helper.buildSpan("hexists", key);
    span.setTag("field", Arrays.toString(field));
    return helper.decorate(span, () -> wrapped.hexists(key, field));
  }

  @Override
  public Long hdel(byte[] key, byte[]... fields) {
    Span span = helper.buildSpan("hdel", key);
    span.setTag("fields", TracingHelper.toString(fields));
    return helper.decorate(span, () -> wrapped.hdel(key, fields));
  }

  @Override
  public Long hlen(byte[] key) {
    Span span = helper.buildSpan("hlen", key);
    return helper.decorate(span, () -> wrapped.hlen(key));
  }

  @Override
  public Set<byte[]> hkeys(byte[] key) {
    Span span = helper.buildSpan("hkeys", key);
    return helper.decorate(span, () -> wrapped.hkeys(key));
  }

  @Override
  public List<byte[]> hvals(byte[] key) {
    Span span = helper.buildSpan("hvals", key);
    return helper.decorate(span, () -> wrapped.hvals(key));
  }

  @Override
  public Map<byte[], byte[]> hgetAll(byte[] key) {
    Span span = helper.buildSpan("hgetAll", key);
    return helper.decorate(span, () -> wrapped.hgetAll(key));
  }

  @Override
  public Long rpush(byte[] key, byte[]... strings) {
    Span span = helper.buildSpan("rpush", key);
    span.setTag("strings", TracingHelper.toString(strings));
    return helper.decorate(span, () -> wrapped.rpush(key, strings));
  }

  @Override
  public Long lpush(byte[] key, byte[]... strings) {
    Span span = helper.buildSpan("lpush", key);
    span.setTag("strings", TracingHelper.toString(strings));
    return helper.decorate(span, () -> wrapped.lpush(key, strings));
  }

  @Override
  public Long llen(byte[] key) {
    Span span = helper.buildSpan("llen", key);
    return helper.decorate(span, () -> wrapped.llen(key));
  }

  @Override
  public List<byte[]> lrange(byte[] key, long start, long end) {
    Span span = helper.buildSpan("lrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.lrange(key, start, end));
  }

  @Override
  public String ltrim(byte[] key, long start, long end) {
    Span span = helper.buildSpan("ltrim", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.ltrim(key, start, end));
  }

  @Override
  public byte[] lindex(byte[] key, long index) {
    Span span = helper.buildSpan("lindex", key);
    span.setTag("index", index);
    return helper.decorate(span, () -> wrapped.lindex(key, index));
  }

  @Override
  public String lset(byte[] key, long index, byte[] value) {
    Span span = helper.buildSpan("lset", key);
    span.setTag("index", index);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> wrapped.lset(key, index, value));
  }

  @Override
  public Long lrem(byte[] key, long count, byte[] value) {
    Span span = helper.buildSpan("lrem", key);
    span.setTag("count", count);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> wrapped.lrem(key, count, value));
  }

  @Override
  public byte[] lpop(byte[] key) {
    Span span = helper.buildSpan("lpop", key);
    return helper.decorate(span, () -> wrapped.lpop(key));
  }

  @Override
  public byte[] rpop(byte[] key) {
    Span span = helper.buildSpan("rpop", key);
    return helper.decorate(span, () -> wrapped.rpop(key));
  }

  @Override
  public byte[] rpoplpush(byte[] srckey, byte[] dstkey) {
    Span span = helper.buildSpan("rpoplpush");
    span.setTag("srckey", Arrays.toString(srckey));
    span.setTag("dstkey", Arrays.toString(dstkey));
    return helper.decorate(span, () -> wrapped.rpoplpush(srckey, dstkey));
  }

  @Override
  public Long sadd(byte[] key, byte[]... members) {
    Span span = helper.buildSpan("sadd", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> wrapped.sadd(key, members));
  }

  @Override
  public Set<byte[]> smembers(byte[] key) {
    Span span = helper.buildSpan("smembers", key);
    return helper.decorate(span, () -> wrapped.smembers(key));
  }

  @Override
  public Long srem(byte[] key, byte[]... member) {
    Span span = helper.buildSpan("srem", key);
    span.setTag("member", Arrays.toString(member));
    return helper.decorate(span, () -> wrapped.srem(key, member));
  }

  @Override
  public byte[] spop(byte[] key) {
    Span span = helper.buildSpan("spop", key);
    return helper.decorate(span, () -> wrapped.spop(key));
  }

  @Override
  public Set<byte[]> spop(byte[] key, long count) {
    Span span = helper.buildSpan("spop", key);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.spop(key, count));
  }

  @Override
  public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
    Span span = helper.buildSpan("smove");
    span.setTag("srckey", Arrays.toString(srckey));
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("member", Arrays.toString(member));
    return helper.decorate(span, () -> wrapped.smove(srckey, dstkey, member));
  }

  @Override
  public Long scard(byte[] key) {
    Span span = helper.buildSpan("scard", key);
    return helper.decorate(span, () -> wrapped.scard(key));
  }

  @Override
  public Boolean sismember(byte[] key, byte[] member) {
    Span span = helper.buildSpan("sismember", key);
    span.setTag("member", Arrays.toString(member));
    return helper.decorate(span, () -> wrapped.sismember(key, member));
  }

  @Override
  public Set<byte[]> sinter(byte[]... keys) {
    Span span = helper.buildSpan("sinter");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> wrapped.sinter(keys));
  }

  @Override
  public Long sinterstore(byte[] dstkey, byte[]... keys) {
    Span span = helper.buildSpan("sinterstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> wrapped.sinterstore(dstkey, keys));
  }

  @Override
  public Set<byte[]> sunion(byte[]... keys) {
    Span span = helper.buildSpan("sunion");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> wrapped.sunion(keys));
  }

  @Override
  public Long sunionstore(byte[] dstkey, byte[]... keys) {
    Span span = helper.buildSpan("sunionstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> wrapped.sunionstore(dstkey, keys));
  }

  @Override
  public Set<byte[]> sdiff(byte[]... keys) {
    Span span = helper.buildSpan("sdiff");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> wrapped.sdiff(keys));
  }

  @Override
  public Long sdiffstore(byte[] dstkey, byte[]... keys) {
    Span span = helper.buildSpan("sdiffstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> wrapped.sdiffstore(dstkey, keys));
  }

  @Override
  public byte[] srandmember(byte[] key) {
    Span span = helper.buildSpan("srandmember", key);
    return helper.decorate(span, () -> wrapped.srandmember(key));
  }

  @Override
  public List<byte[]> srandmember(byte[] key, int count) {
    Span span = helper.buildSpan("srandmember", key);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.srandmember(key, count));
  }

  @Override
  public Long zadd(byte[] key, double score, byte[] member) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("score", score);
    return helper.decorate(span, () -> wrapped.zadd(key, score, member));
  }

  @Override
  public Long zadd(byte[] key, double score, byte[] member, ZAddParams params) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("score", score);
    span.setTag("member", Arrays.toString(member));
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper.decorate(span, () -> wrapped.zadd(key, score, member, params));
  }

  @Override
  public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("scoreMembers", TracingHelper.toStringMap2(scoreMembers));
    return helper.decorate(span, () -> wrapped.zadd(key, scoreMembers));
  }

  @Override
  public Long zadd(byte[] key, Map<byte[], Double> scoreMembers, ZAddParams params) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("scoreMembers", TracingHelper.toStringMap2(scoreMembers));
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper.decorate(span, () -> wrapped.zadd(key, scoreMembers, params));
  }

  @Override
  public Set<byte[]> zrange(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.zrange(key, start, end));
  }

  @Override
  public Long zrem(byte[] key, byte[]... members) {
    Span span = helper.buildSpan("zrem", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> wrapped.zrem(key, members));
  }

  @Override
  public Double zincrby(byte[] key, double score, byte[] member) {
    Span span = helper.buildSpan("zincrby", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("score", score);
    return helper.decorate(span, () -> wrapped.zincrby(key, score, member));
  }

  @Override
  public Double zincrby(byte[] key, double increment, byte[] member, ZIncrByParams params) {
    Span span = helper.buildSpan("zincrby", key);
    span.setTag("increment", increment);
    span.setTag("member", Arrays.toString(member));
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper.decorate(span, () -> wrapped.zincrby(key, increment, member, params));
  }

  @Override
  public Long zrank(byte[] key, byte[] member) {
    Span span = helper.buildSpan("zrank", key);
    span.setTag("member", Arrays.toString(member));
    return helper.decorate(span, () -> wrapped.zrank(key, member));
  }

  @Override
  public Long zrevrank(byte[] key, byte[] member) {
    Span span = helper.buildSpan("zrevrank", key);
    span.setTag("member", Arrays.toString(member));
    return helper.decorate(span, () -> wrapped.zrevrank(key, member));
  }

  @Override
  public Set<byte[]> zrevrange(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zrevrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.zrevrange(key, start, end));
  }

  @Override
  public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.zrangeWithScores(key, start, end));
  }

  @Override
  public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zrevrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.zrevrangeWithScores(key, start, end));
  }

  @Override
  public Long zcard(byte[] key) {
    Span span = helper.buildSpan("zcard", key);
    return helper.decorate(span, () -> wrapped.zcard(key));
  }

  @Override
  public Double zscore(byte[] key, byte[] member) {
    Span span = helper.buildSpan("zscore", key);
    span.setTag("member", Arrays.toString(member));
    return helper.decorate(span, () -> wrapped.zscore(key, member));
  }

  @Override
  public Transaction multi() {
    Span span = helper.buildSpan("multi");
    return helper.decorate(span, () -> wrapped.multi());
  }

  @Override
  protected void checkIsInMultiOrPipeline() {
    Span span = helper.buildSpan("checkisinmultiorpipeline");
    helper.decorate(span, () -> wrapped.checkIsInMultiOrPipeline());
  }

  @Override
  public void connect() {
    Span span = helper.buildSpan("connect");
    helper.decorate(span, () -> wrapped.connect());
  }

  @Override
  public void disconnect() {
    Span span = helper.buildSpan("disconnect");
    helper.decorate(span, () -> wrapped.disconnect());
  }

  @Override
  public void resetState() {
    Span span = helper.buildSpan("resetState");
    helper.decorate(span, () -> wrapped.resetState());
  }

  @Override
  public String watch(byte[]... keys) {
    Span span = helper.buildSpan("watch");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> wrapped.watch(keys));
  }

  @Override
  public String unwatch() {
    Span span = helper.buildSpan("unwatch");
    return helper.decorate(span, () -> wrapped.unwatch());
  }

  @Override
  public List<byte[]> sort(byte[] key) {
    Span span = helper.buildSpan("sort", key);
    return helper.decorate(span, () -> wrapped.sort(key));
  }

  @Override
  public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("sortingParameters", TracingHelper.toString(sortingParameters.getParams()));
    return helper.decorate(span, () -> wrapped.sort(key, sortingParameters));
  }

  @Override
  public List<byte[]> blpop(int timeout, byte[]... keys) {
    Span span = helper.buildSpan("blpop");
    span.setTag("timeout", timeout);
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> wrapped.blpop(timeout, keys));
  }

  @Override
  public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("sortingParameters", TracingHelper.toString(sortingParameters.getParams()));
    span.setTag("dstkey", Arrays.toString(dstkey));
    return helper.decorate(span, () -> wrapped.sort(key, sortingParameters, dstkey));
  }

  @Override
  public Long sort(byte[] key, byte[] dstkey) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("dstkey", Arrays.toString(dstkey));
    return helper.decorate(span, () -> wrapped.sort(key, dstkey));
  }

  @Override
  public List<byte[]> brpop(int timeout, byte[]... keys) {
    Span span = helper.buildSpan("brpop");
    span.setTag("timeout", timeout);
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> wrapped.brpop(timeout, keys));
  }

  @Override
  public List<byte[]> blpop(byte[]... args) {
    Span span = helper.buildSpan("blpop");
    span.setTag("args", TracingHelper.toString(args));
    return helper.decorate(span, () -> wrapped.blpop(args));
  }

  @Override
  public List<byte[]> brpop(byte[]... args) {
    Span span = helper.buildSpan("brpop");
    span.setTag("args", TracingHelper.toString(args));
    return helper.decorate(span, () -> wrapped.brpop(args));
  }

  @Override
  public String auth(String password) {
    Span span = helper.buildSpan("auth");
    return helper.decorate(span, () -> wrapped.auth(password));
  }

  @Override
  public Pipeline pipelined() {
    Span span = helper.buildSpan("pipelined");
    return helper.decorate(span, () -> wrapped.pipelined());
  }

  @Override
  public Long zcount(byte[] key, double min, double max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> wrapped.zcount(key, min, max));
  }

  @Override
  public Long zcount(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    return helper.decorate(span, () -> wrapped.zcount(key, min, max));
  }

  @Override
  public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> wrapped.zrangeByScore(key, min, max));
  }

  @Override
  public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    return helper.decorate(span, () -> wrapped.zrangeByScore(key, min, max));
  }

  @Override
  public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zrangeByScore(key, min, max, offset, count));
  }

  @Override
  public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zrangeByScore(key, min, max, offset, count));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> wrapped.zrangeByScoreWithScores(key, min, max));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    return helper.decorate(span, () -> wrapped.zrangeByScoreWithScores(key, min, max));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset,
      int count) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper
        .decorate(span, () -> wrapped.zrangeByScoreWithScores(key, min, max, offset, count));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset,
      int count) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper
        .decorate(span, () -> wrapped.zrangeByScoreWithScores(key, min, max, offset, count));
  }

  @Override
  protected Set<Tuple> getTupledSet() {
    Span span = helper.buildSpan("gettupledset");
    return helper.decorate(span, () -> wrapped.getTupledSet());
  }

  @Override
  public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> wrapped.zrevrangeByScore(key, max, min));
  }

  @Override
  public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("max", Arrays.toString(max));
    span.setTag("min", Arrays.toString(min));
    return helper.decorate(span, () -> wrapped.zrevrangeByScore(key, max, min));
  }

  @Override
  public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zrevrangeByScore(key, max, min, offset, count));
  }

  @Override
  public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zrevrangeByScore(key, max, min, offset, count));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> wrapped.zrevrangeByScoreWithScores(key, max, min));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset,
      int count) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper
        .decorate(span, () -> wrapped.zrevrangeByScoreWithScores(key, max, min, offset, count));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("max", Arrays.toString(max));
    span.setTag("min", Arrays.toString(min));
    return helper.decorate(span, () -> wrapped.zrevrangeByScoreWithScores(key, max, min));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset,
      int count) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper
        .decorate(span, () -> wrapped.zrevrangeByScoreWithScores(key, max, min, offset, count));
  }

  @Override
  public Long zremrangeByRank(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zremrangeByRank", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.zremrangeByRank(key, start, end));
  }

  @Override
  public Long zremrangeByScore(byte[] key, double start, double end) {
    Span span = helper.buildSpan("zremrangeByScore", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.zremrangeByScore(key, start, end));
  }

  @Override
  public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
    Span span = helper.buildSpan("zremrangeByScore", key);
    span.setTag("start", Arrays.toString(start));
    span.setTag("end", Arrays.toString(end));
    return helper.decorate(span, () -> wrapped.zremrangeByScore(key, start, end));
  }

  @Override
  public Long zunionstore(byte[] dstkey, byte[]... sets) {
    Span span = helper.buildSpan("zunionstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("sets", TracingHelper.toString(sets));
    return helper.decorate(span, () -> wrapped.zunionstore(dstkey, sets));
  }

  @Override
  public Long zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
    Span span = helper.buildSpan("zunionstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    span.setTag("sets", TracingHelper.toString(sets));
    return helper.decorate(span, () -> wrapped.zunionstore(dstkey, params, sets));
  }

  @Override
  public Long zinterstore(byte[] dstkey, byte[]... sets) {
    Span span = helper.buildSpan("zinterstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("sets", TracingHelper.toString(sets));
    return helper.decorate(span, () -> wrapped.zinterstore(dstkey, sets));
  }

  @Override
  public Long zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
    Span span = helper.buildSpan("zinterstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    span.setTag("sets", TracingHelper.toString(sets));
    return helper.decorate(span, () -> wrapped.zinterstore(dstkey, params, sets));
  }

  @Override
  public Long zlexcount(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zlexcount");
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    return helper.decorate(span, () -> wrapped.zlexcount(key, min, max));
  }

  @Override
  public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zrangeByLex", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    return helper.decorate(span, () -> wrapped.zrangeByLex(key, min, max));
  }

  @Override
  public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByLex", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zrangeByLex(key, min, max, offset, count));
  }

  @Override
  public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min) {
    Span span = helper.buildSpan("zrevrangeByLex", key);
    span.setTag("max", Arrays.toString(max));
    span.setTag("min", Arrays.toString(min));
    return helper.decorate(span, () -> wrapped.zrevrangeByLex(key, max, min));
  }

  @Override
  public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByLex", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> wrapped.zrevrangeByLex(key, max, min, offset, count));
  }

  @Override
  public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zremrangeByLex", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    return helper.decorate(span, () -> wrapped.zremrangeByLex(key, min, max));
  }

  @Override
  public String save() {
    Span span = helper.buildSpan("save");
    return helper.decorate(span, () -> wrapped.save());
  }

  @Override
  public String bgsave() {
    Span span = helper.buildSpan("bgsave");
    return helper.decorate(span, () -> wrapped.bgsave());
  }

  @Override
  public String bgrewriteaof() {
    Span span = helper.buildSpan("bgrewriteaof");
    return helper.decorate(span, () -> wrapped.bgrewriteaof());
  }

  @Override
  public Long lastsave() {
    Span span = helper.buildSpan("lastsave");
    return helper.decorate(span, () -> wrapped.lastsave());
  }

  @Override
  public String shutdown() {
    Span span = helper.buildSpan("shutdown");
    return helper.decorate(span, () -> wrapped.shutdown());
  }

  @Override
  public String info() {
    Span span = helper.buildSpan("info");
    return helper.decorate(span, () -> wrapped.info());
  }

  @Override
  public String info(String section) {
    Span span = helper.buildSpan("info");
    span.setTag("section", section);
    return helper.decorate(span, () -> wrapped.info(section));
  }

  @Override
  public void monitor(JedisMonitor jedisMonitor) {
    Span span = helper.buildSpan("monitor");
    helper.decorate(span, () -> wrapped.monitor(jedisMonitor));
  }

  @Override
  public String slaveof(String host, int port) {
    Span span = helper.buildSpan("slaveof");
    span.setTag("host", host);
    span.setTag("port", port);
    return helper.decorate(span, () -> wrapped.slaveof(host, port));
  }

  @Override
  public String slaveofNoOne() {
    Span span = helper.buildSpan("slaveofNoOne");
    return helper.decorate(span, () -> wrapped.slaveofNoOne());
  }

  @Override
  public List<byte[]> configGet(byte[] pattern) {
    Span span = helper.buildSpan("configGet");
    span.setTag("pattern", Arrays.toString(pattern));
    return helper.decorate(span, () -> wrapped.configGet(pattern));
  }

  @Override
  public String configResetStat() {
    Span span = helper.buildSpan("configResetStat");
    return helper.decorate(span, () -> wrapped.configResetStat());
  }

  @Override
  public String configRewrite() {
    return wrapped.configRewrite();
  }

  @Override
  public byte[] configSet(byte[] parameter, byte[] value) {
    Span span = helper.buildSpan("configSet");
    span.setTag("parameter", Arrays.toString(parameter));
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> wrapped.configSet(parameter, value));
  }

  @Override
  public boolean isConnected() {
    Span span = helper.buildSpan("isConnected");
    return helper.decorate(span, () -> wrapped.isConnected());
  }

  @Override
  public Long strlen(byte[] key) {
    Span span = helper.buildSpan("strlen", key);
    return helper.decorate(span, () -> wrapped.strlen(key));
  }

  @Override
  public void sync() {
    Span span = helper.buildSpan("sync");
    helper.decorate(span, () -> wrapped.sync());
  }

  @Override
  public Long lpushx(byte[] key, byte[]... string) {
    Span span = helper.buildSpan("lpushx", key);
    span.setTag("string", TracingHelper.toString(string));
    return helper.decorate(span, () -> wrapped.lpushx(key, string));
  }

  @Override
  public Long persist(byte[] key) {
    Span span = helper.buildSpan("persist", key);
    return helper.decorate(span, () -> wrapped.persist(key));
  }

  @Override
  public Long rpushx(byte[] key, byte[]... string) {
    Span span = helper.buildSpan("rpushx", key);
    span.setTag("string", TracingHelper.toString(string));
    return helper.decorate(span, () -> wrapped.rpushx(key, string));
  }

  @Override
  public byte[] echo(byte[] string) {
    Span span = helper.buildSpan("echo");
    span.setTag("string", Arrays.toString(string));
    return helper.decorate(span, () -> wrapped.echo(string));
  }

  @Override
  public Long linsert(byte[] key, ListPosition where, byte[] pivot, byte[] value) {
    Span span = helper.buildSpan("linsert", key);
    span.setTag("where", where.name());
    span.setTag("pivot", Arrays.toString(pivot));
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> wrapped.linsert(key, where, pivot, value));
  }

  @Override
  public String debug(DebugParams params) {
    Span span = helper.buildSpan("debug");
    span.setTag("params", Arrays.toString(params.getCommand()));
    return helper.decorate(span, () -> wrapped.debug(params));
  }

  @Override
  public Client getClient() {
    Span span = helper.buildSpan("getClient");
    return helper.decorate(span, () -> wrapped.getClient());
  }

  @Override
  public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {
    Span span = helper.buildSpan("brpoplpush");
    span.setTag("timeout", timeout);
    span.setTag("source", Arrays.toString(source));
    span.setTag("destination", Arrays.toString(destination));
    return helper.decorate(span, () -> wrapped.brpoplpush(source, destination, timeout));
  }

  @Override
  public Boolean setbit(byte[] key, long offset, boolean value) {
    Span span = helper.buildSpan("setbit", key);
    span.setTag("offset", offset);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.setbit(key, offset, value));
  }

  @Override
  public Boolean setbit(byte[] key, long offset, byte[] value) {
    Span span = helper.buildSpan("setbit", key);
    span.setTag("offset", offset);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> wrapped.setbit(key, offset, value));
  }

  @Override
  public Boolean getbit(byte[] key, long offset) {
    Span span = helper.buildSpan("getbit", key);
    span.setTag("offset", offset);
    return helper.decorate(span, () -> wrapped.getbit(key, offset));
  }

  @Override
  public Long bitpos(byte[] key, boolean value) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("value", value);
    return helper.decorate(span, () -> wrapped.bitpos(key, value));
  }

  @Override
  public Long bitpos(byte[] key, boolean value, BitPosParams params) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("value", value);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> wrapped.bitpos(key, value, params));
  }

  @Override
  public Long setrange(byte[] key, long offset, byte[] value) {
    Span span = helper.buildSpan("setrange", key);
    span.setTag("offset", offset);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> wrapped.setrange(key, offset, value));
  }

  @Override
  public byte[] getrange(byte[] key, long startOffset, long endOffset) {
    Span span = helper.buildSpan("getrange", key);
    span.setTag("startOffset", startOffset);
    span.setTag("endOffset", endOffset);
    return helper.decorate(span, () -> wrapped.getrange(key, startOffset, endOffset));
  }

  @Override
  public Long publish(byte[] channel, byte[] message) {
    Span span = helper.buildSpan("publish");
    span.setTag("channel", Arrays.toString(channel));
    span.setTag("message", Arrays.toString(message));
    return helper.decorate(span, () -> wrapped.publish(channel, message));
  }

  @Override
  public void subscribe(BinaryJedisPubSub jedisPubSub, byte[]... channels) {
    Span span = helper.buildSpan("subscribe");
    span.setTag("channels", Arrays.toString(channels));
    helper.decorate(span, () -> wrapped.subscribe(jedisPubSub, channels));
  }

  @Override
  public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
    Span span = helper.buildSpan("psubscribe");
    span.setTag("patterns", Arrays.toString(patterns));
    helper.decorate(span, () -> wrapped.psubscribe(jedisPubSub, patterns));
  }

  @Override
  public int getDB() {
    // OT decoration is not needed.
    return wrapped.getDB();
  }

  @Override
  public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
    Span span = helper.buildSpan("eval");
    span.setTag("script", Arrays.toString(script));
    span.setTag("keys", TracingHelper.toString(keys));
    span.setTag("args", TracingHelper.toString(args));
    return helper.decorate(span, () -> wrapped.eval(script, keys, args));
  }

  @Override
  public Object eval(byte[] script, byte[] keyCount, byte[]... params) {
    Span span = helper.buildSpan("eval");
    span.setTag("script", Arrays.toString(script));
    span.setTag("keyCount", Arrays.toString(keyCount));
    span.setTag("params", TracingHelper.toString(params));
    return helper.decorate(span, () -> wrapped.eval(script, keyCount, params));
  }

  @Override
  public Object eval(byte[] script, int keyCount, byte[]... params) {
    Span span = helper.buildSpan("eval");
    span.setTag("script", Arrays.toString(script));
    span.setTag("keyCount", keyCount);
    span.setTag("params", TracingHelper.toString(params));
    return helper.decorate(span, () -> wrapped.eval(script, keyCount, params));
  }

  @Override
  public Object eval(byte[] script) {
    Span span = helper.buildSpan("eval");
    span.setTag("script", Arrays.toString(script));
    return helper.decorate(span, () -> wrapped.eval(script));
  }

  @Override
  public Object evalsha(byte[] sha1) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("sha1", Arrays.toString(sha1));
    return helper.decorate(span, () -> wrapped.evalsha(sha1));
  }

  @Override
  public Object evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("sha1", Arrays.toString(sha1));
    span.setTag("keys", TracingHelper.toString(keys));
    span.setTag("args", TracingHelper.toString(args));
    return helper.decorate(span, () -> wrapped.evalsha(sha1, keys, args));
  }

  @Override
  public Object evalsha(byte[] sha1, int keyCount, byte[]... params) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("params", TracingHelper.toString(params));
    span.setTag("sha1", Arrays.toString(sha1));
    span.setTag("keyCount", keyCount);
    return helper.decorate(span, () -> wrapped.evalsha(sha1, keyCount, params));
  }

  @Override
  public String scriptFlush() {
    Span span = helper.buildSpan("scriptFlush");
    return helper.decorate(span, () -> wrapped.scriptFlush());
  }

  @Override
  public Long scriptExists(byte[] sha1) {
    Span span = helper.buildSpan("scriptExists");
    span.setTag("sha1", Arrays.toString(sha1));
    return helper.decorate(span, () -> wrapped.scriptExists(sha1));
  }

  @Override
  public List<Long> scriptExists(byte[]... sha1) {
    Span span = helper.buildSpan("scriptExists");
    span.setTag("sha1", TracingHelper.toString(sha1));
    return helper.decorate(span, () -> wrapped.scriptExists(sha1));
  }

  @Override
  public byte[] scriptLoad(byte[] script) {
    Span span = helper.buildSpan("scriptLoad");
    span.setTag("script", Arrays.toString(script));
    return helper.decorate(span, () -> wrapped.scriptLoad(script));
  }

  @Override
  public String scriptKill() {
    Span span = helper.buildSpan("scriptKill");
    return helper.decorate(span, () -> wrapped.scriptKill());
  }

  @Override
  public String slowlogReset() {
    Span span = helper.buildSpan("slowlogReset");
    return helper.decorate(span, () -> wrapped.slowlogReset());
  }

  @Override
  public Long slowlogLen() {
    Span span = helper.buildSpan("slowlogLen");
    return helper.decorate(span, () -> wrapped.slowlogLen());
  }

  @Override
  public List<byte[]> slowlogGetBinary() {
    Span span = helper.buildSpan("slowlogGetBinary");
    return helper.decorate(span, () -> wrapped.slowlogGetBinary());
  }

  @Override
  public List<byte[]> slowlogGetBinary(long entries) {
    Span span = helper.buildSpan("slowlogGetBinary");
    span.setTag("entries", entries);
    return helper.decorate(span, () -> wrapped.slowlogGetBinary(entries));
  }

  @Override
  public Long objectRefcount(byte[] key) {
    Span span = helper.buildSpan("objectRefcount", key);
    return helper.decorate(span, () -> wrapped.objectRefcount(key));
  }

  @Override
  public byte[] objectEncoding(byte[] key) {
    Span span = helper.buildSpan("objectEncoding", key);
    return helper.decorate(span, () -> wrapped.objectEncoding(key));
  }

  @Override
  public Long objectIdletime(byte[] key) {
    Span span = helper.buildSpan("objectIdletime", key);
    return helper.decorate(span, () -> wrapped.objectIdletime(key));
  }

  @Override
  public Long bitcount(byte[] key) {
    Span span = helper.buildSpan("bitcount", key);
    return helper.decorate(span, () -> wrapped.bitcount(key));
  }

  @Override
  public Long bitcount(byte[] key, long start, long end) {
    Span span = helper.buildSpan("bitcount", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> wrapped.bitcount(key, start, end));
  }

  @Override
  public Long bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
    Span span = helper.buildSpan("bitop");
    span.setTag("destKey", Arrays.toString(destKey));
    span.setTag("srcKeys", TracingHelper.toString(srcKeys));
    return helper.decorate(span, () -> wrapped.bitop(op, destKey, srcKeys));
  }

  @Override
  public byte[] dump(byte[] key) {
    Span span = helper.buildSpan("dump", key);
    return helper.decorate(span, () -> wrapped.dump(key));
  }

  @Override
  public String restore(byte[] key, int ttl, byte[] serializedValue) {
    Span span = helper.buildSpan("restore", key);
    span.setTag("ttl", ttl);
    span.setTag("serializedValue", Arrays.toString(serializedValue));
    return helper.decorate(span, () -> wrapped.restore(key, ttl, serializedValue));
  }

  @Override
  public String restoreReplace(byte[] key, int ttl, byte[] serializedValue) {
    Span span = helper.buildSpan("restoreReplace", key);
    span.setTag("ttl", ttl);
    span.setTag("serializedValue", Arrays.toString(serializedValue));
    return helper.decorate(span, () -> wrapped.restoreReplace(key, ttl, serializedValue));
  }

  @Override
  public Long pexpire(byte[] key, long milliseconds) {
    Span span = helper.buildSpan("pexpire", key);
    span.setTag("milliseconds", milliseconds);
    return helper.decorate(span, () -> wrapped.pexpire(key, milliseconds));
  }

  @Override
  public Long pexpireAt(byte[] key, long millisecondsTimestamp) {
    Span span = helper.buildSpan("pexpireAt", key);
    span.setTag("millisecondsTimestamp", millisecondsTimestamp);
    return helper.decorate(span, () -> wrapped.pexpireAt(key, millisecondsTimestamp));
  }

  @Override
  public Long pttl(byte[] key) {
    Span span = helper.buildSpan("pttl", key);
    return helper.decorate(span, () -> wrapped.pttl(key));
  }

  @Override
  public String psetex(byte[] key, long milliseconds, byte[] value) {
    Span span = helper.buildSpan("psetex", key);
    span.setTag("milliseconds", milliseconds);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> wrapped.psetex(key, milliseconds, value));
  }

  @Override
  public String clientKill(byte[] client) {
    Span span = helper.buildSpan("clientKill");
    span.setTag("client", Arrays.toString(client));
    return helper.decorate(span, () -> wrapped.clientKill(client));
  }

  @Override
  public String clientKill(String ip, int port) {
    Span span = helper.buildSpan("clientKill");
    span.setTag("ip", ip);
    span.setTag("port", port);
    return helper.decorate(span, () -> wrapped.clientKill(ip, port));
  }

  @Override
  public Long clientKill(ClientKillParams params) {
    Span span = helper.buildSpan("clientKill");
    span.setTag("ip", TracingHelper.toString(params.getByteParams()));
    return helper.decorate(span, () -> wrapped.clientKill(params));
  }

  @Override
  public byte[] clientGetnameBinary() {
    Span span = helper.buildSpan("clientGetnameBinary");
    return helper.decorate(span, wrapped::clientGetnameBinary);
  }

  @Override
  public byte[] clientListBinary() {
    Span span = helper.buildSpan("clientListBinary");
    return helper.decorate(span, wrapped::clientListBinary);
  }

  @Override
  public String clientSetname(byte[] name) {
    Span span = helper.buildSpan("clientSetname");
    span.setTag("name", Arrays.toString(name));
    return helper.decorate(span, () -> wrapped.clientSetname(name));
  }

  @Override
  public String clientPause(long timeout) {
    Span span = helper.buildSpan("clientPause");
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> wrapped.clientPause(timeout));
  }

  @Override
  public List<String> time() {
    Span span = helper.buildSpan("time");
    return helper.decorate(span, () -> wrapped.time());
  }

  @Override
  public String migrate(String host, int port, byte[] key, int destinationDb, int timeout) {
    Span span = helper.buildSpan("migrate", key);
    span.setTag("host", host);
    span.setTag("port", port);
    span.setTag("destinationDb", destinationDb);
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> wrapped.migrate(host, port, key, destinationDb, timeout));
  }

  @Override
  public String migrate(String host, int port, int destinationDB, int timeout, MigrateParams params,
      byte[]... keys) {
    Span span = helper.buildSpan("migrate", keys);
    span.setTag("host", host);
    span.setTag("port", port);
    span.setTag("destinationDB", destinationDB);
    span.setTag("timeout", timeout);
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper
        .decorate(span, () -> wrapped.migrate(host, port, destinationDB, timeout, params, keys));
  }

  @Override
  public Long waitReplicas(int replicas, long timeout) {
    Span span = helper.buildSpan("waitReplicas");
    span.setTag("replicas", replicas);
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> wrapped.waitReplicas(replicas, timeout));
  }

  @Override
  public Long pfadd(byte[] key, byte[]... elements) {
    Span span = helper.buildSpan("pfadd", key);
    span.setTag("elements", TracingHelper.toString(elements));
    return helper.decorate(span, () -> wrapped.pfadd(key, elements));
  }

  @Override
  public long pfcount(byte[] key) {
    Span span = helper.buildSpan("pfcount", key);
    return helper.decorate(span, () -> wrapped.pfcount(key));
  }

  @Override
  public String pfmerge(byte[] destkey, byte[]... sourcekeys) {
    Span span = helper.buildSpan("pfmerge");
    span.setTag("destkey", Arrays.toString(destkey));
    span.setTag("sourcekeys", TracingHelper.toString(sourcekeys));
    return helper.decorate(span, () -> wrapped.pfmerge(destkey, sourcekeys));
  }

  @Override
  public Long pfcount(byte[]... keys) {
    Span span = helper.buildSpan("pfcount");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> wrapped.pfcount(keys));
  }

  @Override
  public ScanResult<byte[]> scan(byte[] cursor) {
    Span span = helper.buildSpan("scan");
    span.setTag("cursor", Arrays.toString(cursor));
    return helper.decorate(span, () -> wrapped.scan(cursor));
  }

  @Override
  public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
    Span span = helper.buildSpan("scan");
    span.setTag("cursor", Arrays.toString(cursor));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> wrapped.scan(cursor, params));
  }

  @Override
  public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    return helper.decorate(span, () -> wrapped.hscan(key, cursor));
  }

  @Override
  public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> wrapped.hscan(key, cursor, params));
  }

  @Override
  public ScanResult<byte[]> sscan(byte[] key, byte[] cursor) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    return helper.decorate(span, () -> wrapped.sscan(key, cursor));
  }

  @Override
  public ScanResult<byte[]> sscan(byte[] key, byte[] cursor, ScanParams params) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> wrapped.sscan(key, cursor, params));
  }

  @Override
  public ScanResult<Tuple> zscan(byte[] key, byte[] cursor) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    return helper.decorate(span, () -> wrapped.zscan(key, cursor));
  }

  @Override
  public ScanResult<Tuple> zscan(byte[] key, byte[] cursor, ScanParams params) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> wrapped.zscan(key, cursor, params));
  }

  @Override
  public Long geoadd(byte[] key, double longitude, double latitude, byte[] member) {
    Span span = helper.buildSpan("geoadd", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("member", Arrays.toString(member));
    return helper.decorate(span, () -> wrapped.geoadd(key, longitude, latitude, member));
  }

  @Override
  public Long geoadd(byte[] key, Map<byte[], GeoCoordinate> memberCoordinateMap) {
    Span span = helper.buildSpan("geoadd", key);
    span.setTag("memberCoordinateMap", TracingHelper.toStringMap2(memberCoordinateMap));
    return helper.decorate(span, () -> wrapped.geoadd(key, memberCoordinateMap));
  }

  @Override
  public Double geodist(byte[] key, byte[] member1, byte[] member2) {
    Span span = helper.buildSpan("geodist", key);
    span.setTag("member1", Arrays.toString(member1));
    span.setTag("member2", Arrays.toString(member2));
    return helper.decorate(span, () -> wrapped.geodist(key, member1, member2));
  }

  @Override
  public Double geodist(byte[] key, byte[] member1, byte[] member2, GeoUnit unit) {
    Span span = helper.buildSpan("geodist", key);
    span.setTag("member1", Arrays.toString(member1));
    span.setTag("member2", Arrays.toString(member2));
    span.setTag("unit", unit.name());
    return helper.decorate(span, () -> wrapped.geodist(key, member1, member2, unit));
  }

  @Override
  public List<byte[]> geohash(byte[] key, byte[]... members) {
    Span span = helper.buildSpan("geohash", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> wrapped.geohash(key, members));
  }

  @Override
  public List<GeoCoordinate> geopos(byte[] key, byte[]... members) {
    Span span = helper.buildSpan("geopos", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> wrapped.geopos(key, members));
  }

  @Override
  public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude,
      double radius, GeoUnit unit) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper.decorate(span, () -> wrapped.georadius(key, longitude, latitude, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadiusReadonly(byte[] key, double longitude, double latitude,
      double radius, GeoUnit unit) {
    Span span = helper.buildSpan("georadiusReadonly", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper
        .decorate(span, () -> wrapped.georadiusReadonly(key, longitude, latitude, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude,
      double radius, GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper
        .decorate(span, () -> wrapped.georadius(key, longitude, latitude, radius, unit, param));
  }

  @Override
  public List<GeoRadiusResponse> georadiusReadonly(byte[] key, double longitude, double latitude,
      double radius, GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadiusReadonly", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper
        .decorate(span,
            () -> wrapped.georadiusReadonly(key, longitude, latitude, radius, unit, param));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius,
      GeoUnit unit) {
    Span span = helper.buildSpan("georadiusByMember", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper.decorate(span, () -> wrapped.georadiusByMember(key, member, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMemberReadonly(byte[] key, byte[] member, double radius,
      GeoUnit unit) {
    Span span = helper.buildSpan("georadiusByMemberReadonly", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper
        .decorate(span, () -> wrapped.georadiusByMemberReadonly(key, member, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius,
      GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadiusByMember", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper
        .decorate(span, () -> wrapped.georadiusByMember(key, member, radius, unit, param));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMemberReadonly(byte[] key, byte[] member, double radius,
      GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadiusByMemberReadonly", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper
        .decorate(span, () -> wrapped.georadiusByMemberReadonly(key, member, radius, unit, param));
  }

  @Override
  public List<Long> bitfield(byte[] key, byte[]... arguments) {
    Span span = helper.buildSpan("bitfield", key);
    span.setTag("arguments", Arrays.toString(arguments));
    return helper.decorate(span, () -> wrapped.bitfield(key, arguments));
  }

  @Override
  public Long hstrlen(byte[] key, byte[] field) {
    Span span = helper.buildSpan("hstrlen", key);
    span.setTag("field", Arrays.toString(field));
    return helper.decorate(span, () -> wrapped.hstrlen(key, field));
  }

  public Jedis getWrapped() {
    return wrapped;
  }
}

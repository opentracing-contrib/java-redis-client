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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;

import io.opentracing.mock.MockTracer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Daniel del Castillo
 */
@ContextConfiguration(classes = {MockConfiguration.class})
@RunWith(SpringJUnit4ClassRunner.class)
public class TracingRedisConnectionTest {

  @Autowired
  MockTracer tracer;

  @Autowired
  RedisConnectionFactory connectionFactory;

  @Autowired
  RedisConnection mockRedisConnection;

  @Before
  public void init() {
    tracer.reset();
  }

  private void commandCreatesNewSpan(String commandName, Runnable command) {
    AssertionUtils.commandCreatesNewSpan(tracer, commandName, command);
  }

  private void commandSpanJoinsActiveSpan(Runnable command) {
    AssertionUtils.commandSpanJoinsActiveSpan(tracer, command);
  }

  protected RedisConnection mockRedisConnection() {
    return mockRedisConnection;
  }

  protected RedisConnection getConnection() {
    return connectionFactory.getConnection();
  }

  /**
   * Make sure we get a {@link RedisConnectionFactory} that returns a tracing connection. Without
   * our Aspect, this would return a regular {@link RedisConnection}.
   */
  @Test
  public void connectionIsInstrumented() {
    assertTrue(connectionFactory.getConnection() instanceof TracingRedisConnection);
  }

  /*
   * The test below verify the creation of a new Span
   */

  @Test
  public void invokingExecuteCreatesNewSpan() {
    commandCreatesNewSpan("custom command", () -> getConnection().execute("custom command"));
    verify(mockRedisConnection()).execute("custom command");
    commandCreatesNewSpan("another command",
        () -> getConnection().execute("another command", "arg".getBytes()));
    verify(mockRedisConnection()).execute("another command", "arg".getBytes());
  }

  @Test
  public void invokingExistsCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.EXISTS, () -> getConnection().exists("key".getBytes()));
    verify(mockRedisConnection()).exists("key".getBytes());
  }

  @Test
  public void invokingDelCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.DEL, () -> getConnection().del("key".getBytes()));
    verify(mockRedisConnection()).del("key".getBytes());
  }

  @Test
  public void invokingTypeCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.TYPE, () -> getConnection().type("key".getBytes()));
    verify(mockRedisConnection()).type("key".getBytes());
  }

  @Test
  public void invokingKeysCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.KEYS, () -> getConnection().keys("pattern".getBytes()));
    verify(mockRedisConnection()).keys("pattern".getBytes());
  }

  @Test
  public void invokingScanCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SCAN, () -> getConnection().scan(ScanOptions.NONE));
    verify(mockRedisConnection()).scan(ScanOptions.NONE);
  }

  @Test
  public void invokingRandomKeyCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.RANDOMKEY, () -> getConnection().randomKey());
    verify(mockRedisConnection()).randomKey();
  }

  @Test
  public void invokingRenameCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.RENAME,
        () -> getConnection().rename("oldName".getBytes(), "newName".getBytes()));
    verify(mockRedisConnection()).rename("oldName".getBytes(), "newName".getBytes());
  }

  @Test
  public void invokingRenameNXCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.RENAMENX,
        () -> getConnection().renameNX("oldName".getBytes(), "newName".getBytes()));
    verify(mockRedisConnection()).renameNX("oldName".getBytes(), "newName".getBytes());
  }

  @Test
  public void invokingExpireCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.EXPIRE, () -> getConnection().expire("key".getBytes(), 0L));
    verify(mockRedisConnection()).expire("key".getBytes(), 0L);
  }

  @Test
  public void invokingPExpireCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.PEXPIRE,
        () -> getConnection().pExpire("key".getBytes(), 0L));
    verify(mockRedisConnection()).pExpire("key".getBytes(), 0L);
  }

  @Test
  public void invokingExpireAtCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.EXPIREAT,
        () -> getConnection().expireAt("key".getBytes(), 0L));
    verify(mockRedisConnection()).expireAt("key".getBytes(), 0L);
  }

  @Test
  public void invokingPExpireAtCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.PEXPIREAT,
        () -> getConnection().pExpireAt("key".getBytes(), 0L));
    verify(mockRedisConnection()).pExpireAt("key".getBytes(), 0L);
  }

  @Test
  public void invokingPersistCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.PERSIST, () -> getConnection().persist("key".getBytes()));
    verify(mockRedisConnection()).persist("key".getBytes());
  }

  @Test
  public void invokingMoveCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.MOVE, () -> getConnection().move("key".getBytes(), 0));
    verify(mockRedisConnection()).move("key".getBytes(), 0);
  }

  @Test
  public void invokingTtlCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.TTL, () -> getConnection().ttl("key".getBytes()));
    verify(mockRedisConnection()).ttl("key".getBytes());
    commandCreatesNewSpan(RedisCommand.TTL,
        () -> getConnection().ttl("key".getBytes(), TimeUnit.SECONDS));
    verify(mockRedisConnection()).ttl("key".getBytes(), TimeUnit.SECONDS);
  }

  @Test
  public void invokingPTtlCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.PTTL, () -> getConnection().pTtl("key".getBytes()));
    verify(mockRedisConnection()).pTtl("key".getBytes());

    commandCreatesNewSpan(RedisCommand.PTTL,
        () -> getConnection().pTtl("key".getBytes(), TimeUnit.SECONDS));
    verify(mockRedisConnection()).pTtl("key".getBytes(), TimeUnit.SECONDS);
  }


  @Test
  public void invokingSortCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SORT, () -> getConnection().sort("key".getBytes(), null));
    verify(mockRedisConnection()).sort("key".getBytes(), null);

    commandCreatesNewSpan(RedisCommand.SORT,
        () -> getConnection().sort("key".getBytes(), null, "sortKey".getBytes()));
    verify(mockRedisConnection()).sort("key".getBytes(), null, "sortKey".getBytes());
  }

  @Test
  public void invokingDumpCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.DUMP, () -> getConnection().dump("key".getBytes()));
    verify(mockRedisConnection()).dump("key".getBytes());
  }

  @Test
  public void invokingRestoreCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.RESTORE,
        () -> getConnection().restore("key".getBytes(), 0L, "val".getBytes()));
    verify(mockRedisConnection()).restore("key".getBytes(), 0L, "val".getBytes());
  }

  @Test
  public void invokingGetCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.GET, () -> getConnection().get("key".getBytes()));
    verify(mockRedisConnection()).get("key".getBytes());
  }

  @Test
  public void invokingGetSetCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.GETSET,
        () -> getConnection().getSet("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).getSet("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingMGetCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.MGET, () -> getConnection().mGet("key".getBytes()));
    verify(mockRedisConnection()).mGet("key".getBytes());
  }

  @Test
  public void invokingSetCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SET,
        () -> getConnection().set("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).set("key".getBytes(), "val".getBytes());

    commandCreatesNewSpan(RedisCommand.SET,
        () -> getConnection().set("key".getBytes(), "val".getBytes(),
            Expiration.persistent(), RedisStringCommands.SetOption.ifAbsent()));
    verify(mockRedisConnection())
        .set(eq("key".getBytes()), eq("val".getBytes()), any(Expiration.class),
            eq(RedisStringCommands.SetOption.ifAbsent()));
  }

  @Test
  public void invokingSetNXCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SETNX,
        () -> getConnection().setNX("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).setNX("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingSetExCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SETEX, () -> getConnection().setEx("key".getBytes(), 0L,
        "val".getBytes()));
    verify(mockRedisConnection()).setEx("key".getBytes(), 0L, "val".getBytes());
  }

  @Test
  public void invokingPSetExCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.PSETEX, () -> getConnection().pSetEx("key".getBytes(), 0L,
        "val".getBytes()));
    verify(mockRedisConnection()).pSetEx("key".getBytes(), 0L, "val".getBytes());
  }

  @Test
  public void invokingMSetCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.MSET, () -> getConnection().mSet(null));
    verify(mockRedisConnection()).mSet(null);
  }

  @Test
  public void invokingMSetNXCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.MSETNX, () -> getConnection().mSetNX(null));
    verify(mockRedisConnection()).mSetNX(null);
  }

  @Test
  public void invokingIncrCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.INCR, () -> getConnection().incr("key".getBytes()));
    verify(mockRedisConnection()).incr("key".getBytes());
  }

  @Test
  public void invokingIncrByCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.INCRBY, () -> getConnection().incrBy("key".getBytes(), 0L));
    verify(mockRedisConnection()).incrBy("key".getBytes(), 0L);

    commandCreatesNewSpan(RedisCommand.INCRBY, () -> getConnection().incrBy("key".getBytes(), 0D));
    verify(mockRedisConnection()).incrBy("key".getBytes(), 0D);
  }

  @Test
  public void invokingDecrCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.DECR, () -> getConnection().decr("key".getBytes()));
    verify(mockRedisConnection()).decr("key".getBytes());
  }

  @Test
  public void invokingDecrByCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.DECRBY, () -> getConnection().decrBy("key".getBytes(), 0L));
    verify(mockRedisConnection()).decrBy("key".getBytes(), 0L);
  }

  @Test
  public void invokingAppendCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.APPEND,
        () -> getConnection().append("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).append("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingGetRangeCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.GETRANGE,
        () -> getConnection().getRange("key".getBytes(), 0L, 0L));
    verify(mockRedisConnection()).getRange("key".getBytes(), 0L, 0L);
  }

  @Test
  public void invokingSetRangeCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SETRANGE,
        () -> getConnection().setRange("key".getBytes(), "val".getBytes(), 0L));
    verify(mockRedisConnection()).setRange("key".getBytes(), "val".getBytes(), 0L);
  }

  @Test
  public void invokingGetBitCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.GETBIT, () -> getConnection().getBit("key".getBytes(), 0L));
    verify(mockRedisConnection()).getBit("key".getBytes(), 0L);
  }

  @Test
  public void invokingSetBitCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SETBIT,
        () -> getConnection().setBit("key".getBytes(), 0L, false));
    verify(mockRedisConnection()).setBit("key".getBytes(), 0L, false);
  }

  @Test
  public void invokingBitCountCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.BITCOUNT, () -> getConnection().bitCount("key".getBytes()));
    verify(mockRedisConnection()).bitCount("key".getBytes());

    commandCreatesNewSpan(RedisCommand.BITCOUNT,
        () -> getConnection().bitCount("key".getBytes(), 0L, 0L));
    verify(mockRedisConnection()).bitCount("key".getBytes(), 0L, 0L);
  }

  @Test
  public void invokingBitOpCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.BITOP,
        () -> getConnection().bitOp(RedisStringCommands.BitOperation.OR,
            "dst".getBytes(), "key".getBytes()));
    verify(mockRedisConnection()).bitOp(RedisStringCommands.BitOperation.OR,
        "dst".getBytes(), "key".getBytes());
  }

  @Test
  public void invokingStrLenCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.STRLEN, () -> getConnection().strLen("key".getBytes()));
    verify(mockRedisConnection()).strLen("key".getBytes());
  }

  @Test
  public void invokingRPushCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.RPUSH,
        () -> getConnection().rPush("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).rPush("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingLPushCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.LPUSH,
        () -> getConnection().lPush("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).lPush("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingRPushXCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.RPUSHX,
        () -> getConnection().rPushX("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).rPushX("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingLPushXCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.LPUSHX,
        () -> getConnection().lPushX("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).lPushX("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingLLenCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.LLEN, () -> getConnection().lLen("key".getBytes()));
    verify(mockRedisConnection()).lLen("key".getBytes());
  }

  @Test
  public void invokingLRangeCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.LRANGE,
        () -> getConnection().lRange("key".getBytes(), 0L, 0L));
    verify(mockRedisConnection()).lRange("key".getBytes(), 0L, 0L);
  }

  @Test
  public void invokingLTrimCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.LTRIM,
        () -> getConnection().lTrim("key".getBytes(), 0L, 0L));
    verify(mockRedisConnection()).lTrim("key".getBytes(), 0L, 0L);
  }

  @Test
  public void invokingLIndexCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.LINDEX, () -> getConnection().lIndex("key".getBytes(), 0L));
    verify(mockRedisConnection()).lIndex("key".getBytes(), 0L);
  }

  @Test
  public void invokingLInsertCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.LINSERT, () -> getConnection().lInsert("key".getBytes(),
        RedisListCommands.Position.AFTER, "pivot".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).lInsert("key".getBytes(),
        RedisListCommands.Position.AFTER, "pivot".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingLSetCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.LSET,
        () -> getConnection().lSet("key".getBytes(), 0L, "val".getBytes()));
    verify(mockRedisConnection()).lSet("key".getBytes(), 0L, "val".getBytes());
  }

  @Test
  public void invokingLRemCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.LREM,
        () -> getConnection().lRem("key".getBytes(), 0L, "val".getBytes()));
    verify(mockRedisConnection()).lRem("key".getBytes(), 0L, "val".getBytes());
  }

  @Test
  public void invokingLPopCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.LPOP, () -> getConnection().lPop("key".getBytes()));
    verify(mockRedisConnection()).lPop("key".getBytes());
  }

  @Test
  public void invokingRPopCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.RPOP, () -> getConnection().rPop("key".getBytes()));
    verify(mockRedisConnection()).rPop("key".getBytes());
  }

  @Test
  public void invokingBLPopCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.BLPOP, () -> getConnection().bLPop(0, "key".getBytes()));
    verify(mockRedisConnection()).bLPop(0, "key".getBytes());
  }

  @Test
  public void invokingBRPopCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.BRPOP, () -> getConnection().bRPop(0, "key".getBytes()));
    verify(mockRedisConnection()).bRPop(0, "key".getBytes());
  }

  @Test
  public void invokingRPopLPushCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.RPOPLPUSH,
        () -> getConnection().rPopLPush("src".getBytes(), "dst".getBytes()));
    verify(mockRedisConnection()).rPopLPush("src".getBytes(), "dst".getBytes());
  }

  @Test
  public void invokingBRPopLPushCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.BRPOPLPUSH,
        () -> getConnection().bRPopLPush(0, "src".getBytes(), "dst".getBytes()));
    verify(mockRedisConnection()).bRPopLPush(0, "src".getBytes(), "dst".getBytes());
  }

  @Test
  public void invokingSAddCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SADD,
        () -> getConnection().sAdd("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).sAdd("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingSRemCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SREM,
        () -> getConnection().sRem("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).sRem("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingSPopCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SPOP, () -> getConnection().sPop("key".getBytes()));
    verify(mockRedisConnection()).sPop("key".getBytes());

    commandCreatesNewSpan(RedisCommand.SPOP, () -> getConnection().sPop("key".getBytes(), 0L));
    verify(mockRedisConnection()).sPop("key".getBytes(), 0L);
  }

  @Test
  public void invokingSMoveCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SMOVE,
        () -> getConnection().sMove("src".getBytes(), "dst".getBytes(),
            "val".getBytes()));
    verify(mockRedisConnection()).sMove("src".getBytes(), "dst".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingSCardCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SCARD, () -> getConnection().sCard("key".getBytes()));
    verify(mockRedisConnection()).sCard("key".getBytes());
  }

  @Test
  public void invokingSIsMemberCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SISMEMBER,
        () -> getConnection().sIsMember("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).sIsMember("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingSInterCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SINTER, () -> getConnection().sInter("key".getBytes()));
    verify(mockRedisConnection()).sInter("key".getBytes());
  }

  @Test
  public void invokingSInterStoreCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SINTERSTORE,
        () -> getConnection().sInterStore("dst".getBytes(),
            "key".getBytes()));
    verify(mockRedisConnection()).sInterStore("dst".getBytes(), "key".getBytes());
  }

  @Test
  public void invokingSUnionCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SUNION, () -> getConnection().sUnion("key".getBytes()));
    verify(mockRedisConnection()).sUnion("key".getBytes());
  }

  @Test
  public void invokingSUnionStoreCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SUNIONSTORE,
        () -> getConnection().sUnionStore("dst".getBytes(),
            "key".getBytes()));
    verify(mockRedisConnection()).sUnionStore("dst".getBytes(), "key".getBytes());
  }

  @Test
  public void invokingSDiffCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SDIFF, () -> getConnection().sDiff("key".getBytes()));
    verify(mockRedisConnection()).sDiff("key".getBytes());
  }

  @Test
  public void invokingSDiffStoreCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SDIFFSTORE,
        () -> getConnection().sDiffStore("dst".getBytes(), "key".getBytes()));
    verify(mockRedisConnection()).sDiffStore("dst".getBytes(), "key".getBytes());
  }

  @Test
  public void invokingSMembersCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SMEMBERS, () -> getConnection().sMembers("key".getBytes()));
    verify(mockRedisConnection()).sMembers("key".getBytes());
  }

  @Test
  public void invokingSRandMemberCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SRANDMEMBER,
        () -> getConnection().sRandMember("key".getBytes()));
    verify(mockRedisConnection()).sRandMember("key".getBytes());

    commandCreatesNewSpan(RedisCommand.SRANDMEMBER,
        () -> getConnection().sRandMember("key".getBytes(), 0L));
    verify(mockRedisConnection()).sRandMember("key".getBytes(), 0L);
  }

  @Test
  public void invokingSScanCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SSCAN,
        () -> getConnection().sScan("key".getBytes(), ScanOptions.NONE));
    verify(mockRedisConnection()).sScan("key".getBytes(), ScanOptions.NONE);
  }

  @Test
  public void invokingZAddCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZADD,
        () -> getConnection().zAdd("key".getBytes(), 0D, "val".getBytes()));
    verify(mockRedisConnection()).zAdd("key".getBytes(), 0D, "val".getBytes());

    commandCreatesNewSpan(RedisCommand.ZADD, () -> getConnection().zAdd("key".getBytes(), null));
    verify(mockRedisConnection()).zAdd("key".getBytes(), null);
  }

  @Test
  public void invokingZRemCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZREM,
        () -> getConnection().zRem("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).zRem("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingZIncrByCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZINCRBY,
        () -> getConnection().zIncrBy("key".getBytes(), 0D, "val".getBytes()));
    verify(mockRedisConnection()).zIncrBy("key".getBytes(), 0D, "val".getBytes());
  }

  @Test
  public void invokingZRankCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZRANK,
        () -> getConnection().zRank("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).zRank("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingZRevRankCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZREVRANK,
        () -> getConnection().zRevRank("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).zRevRank("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingZRangeCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZRANGE,
        () -> getConnection().zRange("key".getBytes(), 0L, 0L));
    verify(mockRedisConnection()).zRange("key".getBytes(), 0L, 0L);
  }

  @Test
  public void invokingZRevRangeCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZREVRANGE,
        () -> getConnection().zRevRange("key".getBytes(), 0L, 0L));
    verify(mockRedisConnection()).zRevRange("key".getBytes(), 0L, 0L);
  }

  @Test
  public void invokingZRevRangeWithScoresCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZREVRANGE_WITHSCORES,
        () -> getConnection().zRevRangeWithScores("key".getBytes(),
            0L, 0L));
    verify(mockRedisConnection()).zRevRangeWithScores("key".getBytes(), 0L, 0L);
  }

  @Test
  public void invokingZRangeWithScoresCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZRANGE_WITHSCORES,
        () -> getConnection().zRangeWithScores("key".getBytes(), 0L,
            0L));
    verify(mockRedisConnection()).zRangeWithScores("key".getBytes(), 0L, 0L);

    commandCreatesNewSpan(RedisCommand.ZRANGEBYSCORE,
        () -> getConnection().zRangeByScore("key".getBytes(), 0D,
            0D));
    verify(mockRedisConnection()).zRangeByScore("key".getBytes(), 0D, 0D);
  }

  @Test
  public void invokingZRangeByScoreCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZRANGEBYSCORE,
        () -> getConnection().zRangeByScore("key".getBytes(), 0D, 0D, 0L,
            0L));
    verify(mockRedisConnection()).zRangeByScore("key".getBytes(), 0D, 0D, 0L, 0L);

    commandCreatesNewSpan(RedisCommand.ZRANGEBYSCORE,
        () -> getConnection().zRangeByScore("key".getBytes(),
            "min", "max"));
    verify(mockRedisConnection()).zRangeByScore("key".getBytes(), "min", "max");

    commandCreatesNewSpan(RedisCommand.ZRANGEBYSCORE,
        () -> getConnection().zRangeByScore("key".getBytes(),
            RedisZSetCommands.Range.range()));
    verify(mockRedisConnection())
        .zRangeByScore(eq("key".getBytes()), any(RedisZSetCommands.Range.class));

    commandCreatesNewSpan(RedisCommand.ZRANGEBYSCORE,
        () -> getConnection().zRangeByScore("key".getBytes(),
            "min", "max", 0L, 0L));
    verify(mockRedisConnection()).zRangeByScore("key".getBytes(), "min", "max", 0L, 0L);

    commandCreatesNewSpan(RedisCommand.ZRANGEBYSCORE,
        () -> getConnection().zRangeByScore("key".getBytes(),
            RedisZSetCommands.Range.range(), RedisZSetCommands.Limit.limit()));
    verify(mockRedisConnection())
        .zRangeByScore(eq("key".getBytes()), any(RedisZSetCommands.Range.class),
            any(RedisZSetCommands.Limit.class));
  }

  @Test
  public void invokingZRangeByScoreWithScoresCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZRANGEBYSCORE_WITHSCORES,
        () -> getConnection()
            .zRangeByScoreWithScores("key".getBytes(), RedisZSetCommands.Range.range()));
    verify(mockRedisConnection())
        .zRangeByScoreWithScores(eq("key".getBytes()), any(RedisZSetCommands.Range.class));

    commandCreatesNewSpan(RedisCommand.ZRANGEBYSCORE_WITHSCORES,
        () -> getConnection().zRangeByScoreWithScores("key".getBytes(), 0D, 0D));
    verify(mockRedisConnection()).zRangeByScoreWithScores("key".getBytes(), 0D, 0D);

    commandCreatesNewSpan(RedisCommand.ZRANGEBYSCORE_WITHSCORES,
        () -> getConnection().zRangeByScoreWithScores("key".getBytes(), 0D, 0D, 0L, 0L));
    verify(mockRedisConnection()).zRangeByScoreWithScores("key".getBytes(), 0D, 0D, 0L, 0L);

    commandCreatesNewSpan(RedisCommand.ZRANGEBYSCORE_WITHSCORES,
        () -> getConnection()
            .zRangeByScoreWithScores("key".getBytes(), RedisZSetCommands.Range.range(),
                RedisZSetCommands.Limit.limit()));
    verify(mockRedisConnection())
        .zRangeByScoreWithScores(eq("key".getBytes()), any(RedisZSetCommands.Range.class),
            any(RedisZSetCommands.Limit.class));
  }

  @Test
  public void invokingZRevRangeByScoreCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZREVRANGEBYSCORE,
        () -> getConnection().zRevRangeByScore("key".getBytes(), 0D, 0D));
    verify(mockRedisConnection()).zRevRangeByScore("key".getBytes(), 0D, 0D);

    commandCreatesNewSpan(RedisCommand.ZREVRANGEBYSCORE,
        () -> getConnection().zRevRangeByScore("key".getBytes(),
            RedisZSetCommands.Range.range()));
    verify(mockRedisConnection())
        .zRevRangeByScore(eq("key".getBytes()), any(RedisZSetCommands.Range.class));

    commandCreatesNewSpan(RedisCommand.ZREVRANGEBYSCORE,
        () -> getConnection().zRevRangeByScore("key".getBytes(), 0D, 0D, 0L, 0L));
    verify(mockRedisConnection()).zRevRangeByScore("key".getBytes(), 0D, 0D, 0L, 0L);

    commandCreatesNewSpan(RedisCommand.ZREVRANGEBYSCORE,
        () -> getConnection().zRevRangeByScore("key".getBytes(), RedisZSetCommands.Range.range(),
            RedisZSetCommands.Limit.limit()));
    verify(mockRedisConnection())
        .zRevRangeByScore(eq("key".getBytes()), any(RedisZSetCommands.Range.class),
            any(RedisZSetCommands.Limit.class));
  }

  @Test
  public void invokingZRevRangeByScoreWithScoresCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
        () -> getConnection().zRevRangeByScoreWithScores("key".getBytes(), 0D, 0D));
    verify(mockRedisConnection()).zRevRangeByScoreWithScores("key".getBytes(), 0D, 0D);

    commandCreatesNewSpan(RedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
        () -> getConnection().zRevRangeByScoreWithScores("key".getBytes(), 0D, 0D, 0L, 0L));
    verify(mockRedisConnection()).zRevRangeByScoreWithScores("key".getBytes(), 0D, 0D, 0L, 0L);

    commandCreatesNewSpan(RedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
        () -> getConnection().zRevRangeByScoreWithScores("key".getBytes(),
            RedisZSetCommands.Range.range()));
    verify(mockRedisConnection()).zRevRangeByScoreWithScores(eq("key".getBytes()),
        any(RedisZSetCommands.Range.class));

    commandCreatesNewSpan(RedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
        () -> getConnection()
            .zRevRangeByScoreWithScores("key".getBytes(), RedisZSetCommands.Range.range(),
                RedisZSetCommands.Limit.limit()));
    verify(mockRedisConnection())
        .zRevRangeByScoreWithScores(eq("key".getBytes()), any(RedisZSetCommands.Range.class),
            any(RedisZSetCommands.Limit.class));
  }

  @Test
  public void invokingZCountCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZCOUNT,
        () -> getConnection().zCount("key".getBytes(), 0D, 0D));
    verify(mockRedisConnection()).zCount("key".getBytes(), 0D, 0D);

    commandCreatesNewSpan(RedisCommand.ZCOUNT, () -> getConnection().zCount("key".getBytes(),
        RedisZSetCommands.Range.range()));
    verify(mockRedisConnection()).zCount(eq("key".getBytes()), any(RedisZSetCommands.Range.class));
  }

  @Test
  public void invokingZCardCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZCARD, () -> getConnection().zCard("key".getBytes()));
    verify(mockRedisConnection()).zCard("key".getBytes());
  }

  @Test
  public void invokingZScoreCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZSCORE,
        () -> getConnection().zScore("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).zScore("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingZRemRangeCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZREMRANGE,
        () -> getConnection().zRemRange("key".getBytes(), 0L, 0L));
    verify(mockRedisConnection()).zRemRange("key".getBytes(), 0L, 0L);
  }

  @Test
  public void invokingZRemRangeByScoreCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZREMRANGEBYSCORE,
        () -> getConnection().zRemRangeByScore("key".getBytes(), 0D, 0D));
    verify(mockRedisConnection()).zRemRangeByScore("key".getBytes(), 0D, 0D);

    commandCreatesNewSpan(RedisCommand.ZREMRANGEBYSCORE,
        () -> getConnection().zRemRangeByScore("key".getBytes(),
            RedisZSetCommands.Range.range()));
    verify(mockRedisConnection())
        .zRemRangeByScore(eq("key".getBytes()), any(RedisZSetCommands.Range.class));
  }

  @Test
  public void invokingZUnionStoreCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZUNIONSTORE,
        () -> getConnection().zUnionStore("dst".getBytes(),
            "set".getBytes()));
    verify(mockRedisConnection()).zUnionStore("dst".getBytes(), "set".getBytes());

    commandCreatesNewSpan(RedisCommand.ZUNIONSTORE,
        () -> getConnection().zUnionStore("dst".getBytes(),
            RedisZSetCommands.Aggregate.SUM, new int[]{0}, "set".getBytes()));
    verify(mockRedisConnection())
        .zUnionStore("dst".getBytes(), RedisZSetCommands.Aggregate.SUM, new int[]{0},
            "set".getBytes());
  }

  @Test
  public void invokingZInterStoreCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZINTERSTORE,
        () -> getConnection().zInterStore("dst".getBytes(), "set".getBytes()));
    verify(mockRedisConnection()).zInterStore("dst".getBytes(), "set".getBytes());

    commandCreatesNewSpan(RedisCommand.ZINTERSTORE,
        () -> getConnection().zInterStore("dst".getBytes(),
            RedisZSetCommands.Aggregate.SUM, new int[]{0}, "set".getBytes()));
    verify(mockRedisConnection())
        .zInterStore("dst".getBytes(), RedisZSetCommands.Aggregate.SUM, new int[]{0},
            "set".getBytes());
  }

  @Test
  public void invokingZScanCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZSCAN,
        () -> getConnection().zScan("key".getBytes(), ScanOptions.NONE));
    verify(mockRedisConnection()).zScan("key".getBytes(), ScanOptions.NONE);
  }

  @Test
  public void invokingZRangeByLexCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ZRANGEBYLEX,
        () -> getConnection().zRangeByLex("key".getBytes()));
    verify(mockRedisConnection()).zRangeByLex("key".getBytes());

    commandCreatesNewSpan(RedisCommand.ZRANGEBYLEX,
        () -> getConnection().zRangeByLex("key".getBytes(),
            RedisZSetCommands.Range.range()));
    verify(mockRedisConnection())
        .zRangeByLex(eq("key".getBytes()), any(RedisZSetCommands.Range.class));

    commandCreatesNewSpan(RedisCommand.ZRANGEBYLEX,
        () -> getConnection().zRangeByLex("key".getBytes(),
            RedisZSetCommands.Range.range(), RedisZSetCommands.Limit.limit()));
    verify(mockRedisConnection())
        .zRangeByLex(eq("key".getBytes()), any(RedisZSetCommands.Range.class),
            any(RedisZSetCommands.Limit.class));
  }

  @Test
  public void invokingHSetCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.HSET,
        () -> getConnection().hSet("key".getBytes(), "field".getBytes(),
            "val".getBytes()));
    verify(mockRedisConnection()).hSet("key".getBytes(), "field".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingHSetNXCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.HSETNX, () -> getConnection().hSetNX("key".getBytes(),
        "field".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).hSetNX("key".getBytes(), "field".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingHGetCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.HGET,
        () -> getConnection().hGet("key".getBytes(), "field".getBytes()));
    verify(mockRedisConnection()).hGet("key".getBytes(), "field".getBytes());
  }

  @Test
  public void invokingHMGetCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.HMGET,
        () -> getConnection().hMGet("key".getBytes(), "field".getBytes()));
    verify(mockRedisConnection()).hMGet("key".getBytes(), "field".getBytes());
  }

  @Test
  public void invokingHMSetCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.HMSET, () -> getConnection().hMSet("key".getBytes(), null));
    verify(mockRedisConnection()).hMSet("key".getBytes(), null);
  }

  @Test
  public void invokingHIncrByCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.HINCRBY, () -> getConnection().hIncrBy("key".getBytes(),
        "field".getBytes(), 0L));
    verify(mockRedisConnection()).hIncrBy("key".getBytes(), "field".getBytes(), 0L);

    commandCreatesNewSpan(RedisCommand.HINCRBY, () -> getConnection().hIncrBy("key".getBytes(),
        "field".getBytes(), 0D));
    verify(mockRedisConnection()).hIncrBy("key".getBytes(), "field".getBytes(), 0D);
  }

  @Test
  public void invokingHExistsCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.HEXISTS,
        () -> getConnection().hExists("key".getBytes(), "field".getBytes()));
    verify(mockRedisConnection()).hExists("key".getBytes(), "field".getBytes());
  }

  @Test
  public void invokingHDelCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.HDEL,
        () -> getConnection().hDel("key".getBytes(), "field".getBytes()));
    verify(mockRedisConnection()).hDel("key".getBytes(), "field".getBytes());
  }

  @Test
  public void invokingHLenCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.HLEN, () -> getConnection().hLen("key".getBytes()));
    verify(mockRedisConnection()).hLen("key".getBytes());
  }

  @Test
  public void invokingHKeysCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.HKEYS, () -> getConnection().hKeys("key".getBytes()));
    verify(mockRedisConnection()).hKeys("key".getBytes());
  }

  @Test
  public void invokingHValsCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.HVALS, () -> getConnection().hVals("key".getBytes()));
    verify(mockRedisConnection()).hVals("key".getBytes());
  }

  @Test
  public void invokingHGetAllCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.HGETALL, () -> getConnection().hGetAll("key".getBytes()));
    verify(mockRedisConnection()).hGetAll("key".getBytes());
  }

  @Test
  public void invokingHScanCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.HSCAN,
        () -> getConnection().hScan("key".getBytes(), ScanOptions.NONE));
    verify(mockRedisConnection()).hScan("key".getBytes(), ScanOptions.NONE);
  }

  @Test
  public void invokingMultiCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.MULTI, () -> getConnection().multi());
    verify(mockRedisConnection()).multi();
  }

  @Test
  public void invokingExecCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.EXEC, () -> getConnection().exec());
    verify(mockRedisConnection()).exec();
  }

  @Test
  public void invokingDiscardCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.DISCARD, () -> getConnection().discard());
    verify(mockRedisConnection()).discard();
  }

  @Test
  public void invokingWatchCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.WATCH, () -> getConnection().watch("key".getBytes()));
    verify(mockRedisConnection()).watch("key".getBytes());
  }

  @Test
  public void invokingUnwatchCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.UNWATCH, () -> getConnection().unwatch());
    verify(mockRedisConnection()).unwatch();
  }

  @Test
  public void invokingPublishCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.PUBLISH, () -> getConnection().publish("channel".getBytes(),
        "message".getBytes()));
    verify(mockRedisConnection()).publish("channel".getBytes(), "message".getBytes());
  }

  @Test
  public void invokingSubscribeCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SUBSCRIBE,
        () -> getConnection().subscribe(null, "channel".getBytes()));
    verify(mockRedisConnection()).subscribe(null, "channel".getBytes());
  }

  @Test
  public void invokingPpSubscribeCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.PSUBSCRIBE,
        () -> getConnection().pSubscribe(null, "pattern".getBytes()));
    verify(mockRedisConnection()).pSubscribe(null, "pattern".getBytes());
  }

  @Test
  public void invokingSelectCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SELECT, () -> getConnection().select(0));
    verify(mockRedisConnection()).select(0);
  }

  @Test
  public void invokingEchoCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.ECHO, () -> getConnection().echo("message".getBytes()));
    verify(mockRedisConnection()).echo("message".getBytes());
  }

  @Test
  public void invokingPingCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.PING, () -> getConnection().ping());
    verify(mockRedisConnection()).ping();
  }

  @Test
  public void invokingBgWriteAofCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.BGWRITEAOF, () -> getConnection().bgWriteAof());
    verify(mockRedisConnection()).bgWriteAof();
  }

  @Test
  public void invokingBgReWriteAofCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.BGREWRITEAOF, () -> getConnection().bgReWriteAof());
    verify(mockRedisConnection()).bgReWriteAof();
  }

  @Test
  public void invokingBgSaveCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.BGSAVE, () -> getConnection().bgSave());
    verify(mockRedisConnection()).bgSave();
  }

  @Test
  public void invokingLastSaveCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.LASTSAVE, () -> getConnection().lastSave());
    verify(mockRedisConnection()).lastSave();
  }

  @Test
  public void invokingSaveCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SAVE, () -> getConnection().save());
    verify(mockRedisConnection()).save();
  }

  @Test
  public void invokingDbSizeCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.DBSIZE, () -> getConnection().dbSize());
    verify(mockRedisConnection()).dbSize();
  }

  @Test
  public void invokingFlushDbCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.FLUSHDB, () -> getConnection().flushDb());
    verify(mockRedisConnection()).flushDb();
  }

  @Test
  public void invokingFlushAllCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.FLUSHALL, () -> getConnection().flushAll());
    verify(mockRedisConnection()).flushAll();
  }

  @Test
  public void invokingInfoCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.INFO, () -> getConnection().info());
    verify(mockRedisConnection()).info();

    commandCreatesNewSpan(RedisCommand.INFO, () -> getConnection().info("section"));
    verify(mockRedisConnection()).info("section");
  }

  @Test
  public void invokingShutdownCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SHUTDOWN, () -> getConnection().shutdown());
    verify(mockRedisConnection()).shutdown();

    commandCreatesNewSpan(RedisCommand.SHUTDOWN,
        () -> getConnection().shutdown(RedisServerCommands.ShutdownOption.SAVE));
    verify(mockRedisConnection()).shutdown(RedisServerCommands.ShutdownOption.SAVE);
  }

  @Test
  public void invokingGetConfigCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.CONFIG_GET, () -> getConnection().getConfig("pattern"));
    verify(mockRedisConnection()).getConfig("pattern");
  }

  @Test
  public void invokingSetConfiCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.CONFIG_SET, () -> getConnection().setConfig("param", "val"));
    verify(mockRedisConnection()).setConfig("param", "val");
  }

  @Test
  public void invokingResetConfigStatsCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.CONFIG_RESETSTAT, () -> getConnection().resetConfigStats());
    verify(mockRedisConnection()).resetConfigStats();
  }

  @Test
  public void invokingTimeCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.TIME, () -> getConnection().time());
    verify(mockRedisConnection()).time();
  }

  @Test
  public void invokingKillClientCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.CLIENT_KILL, () -> getConnection().killClient("host", 0));
    verify(mockRedisConnection()).killClient("host", 0);
  }

  @Test
  public void invokingSetClientNameCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.CLIENT_SETNAME,
        () -> getConnection().setClientName("name".getBytes()));
    verify(mockRedisConnection()).setClientName("name".getBytes());
  }

  @Test
  public void invokingGetClientNameCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.CLIENT_GETNAME, () -> getConnection().getClientName());
    verify(mockRedisConnection()).getClientName();
  }

  @Test
  public void invokingGetClientListCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.CLIENT_LIST, () -> getConnection().getClientList());
    verify(mockRedisConnection()).getClientList();
  }

  @Test
  public void invokingSlaveOfCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SLAVEOF, () -> getConnection().slaveOf("host", 0));
    verify(mockRedisConnection()).slaveOf("host", 0);
  }

  @Test
  public void invokingSlaveOfNoOneCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SLAVEOFNOONE, () -> getConnection().slaveOfNoOne());
    verify(mockRedisConnection()).slaveOfNoOne();
  }

  @Test
  public void invokingMigrateCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.MIGRATE,
        () -> getConnection().migrate("key".getBytes(), null, 0,
            RedisServerCommands.MigrateOption.REPLACE));
    verify(mockRedisConnection())
        .migrate("key".getBytes(), null, 0, RedisServerCommands.MigrateOption.REPLACE);

    commandCreatesNewSpan(RedisCommand.MIGRATE,
        () -> getConnection().migrate("key".getBytes(), null, 0,
            RedisServerCommands.MigrateOption.REPLACE, 0L));
    verify(mockRedisConnection())
        .migrate("key".getBytes(), null, 0, RedisServerCommands.MigrateOption.REPLACE, 0L);
  }

  @Test
  public void invokingScriptFlushCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SCRIPT_FLUSH, () -> getConnection().scriptFlush());
    verify(mockRedisConnection()).scriptFlush();
  }

  @Test
  public void invokingScriptKillCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SCRIPT_KILL, () -> getConnection().scriptKill());
    verify(mockRedisConnection()).scriptKill();
  }

  @Test
  public void invokingScriptLoadCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SCRIPT_LOAD,
        () -> getConnection().scriptLoad("script".getBytes()));
    verify(mockRedisConnection()).scriptLoad("script".getBytes());
  }

  @Test
  public void invokingScriptExistsCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.SCRIPT_EXISTS,
        () -> getConnection().scriptExists("scriptSha"));
    verify(mockRedisConnection()).scriptExists("scriptSha");
  }

  @Test
  public void invokingEvalCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.EVAL,
        () -> getConnection().eval("script".getBytes(), ReturnType.MULTI,
            1, "keysAndArgs".getBytes()));
    verify(mockRedisConnection())
        .eval("script".getBytes(), ReturnType.MULTI, 1, "keysAndArgs".getBytes());
  }

  @Test
  public void invokingEvalShaCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.EVALSHA,
        () -> getConnection()
            .evalSha("scriptSha", ReturnType.BOOLEAN, 1, "keysAndArgs".getBytes()));
    verify(mockRedisConnection())
        .evalSha("scriptSha", ReturnType.BOOLEAN, 1, "keysAndArgs".getBytes());

    commandCreatesNewSpan(RedisCommand.EVALSHA,
        () -> getConnection().evalSha("scriptSha".getBytes(), ReturnType.BOOLEAN, 1,
            "keysAndArgs".getBytes()));
    verify(mockRedisConnection())
        .evalSha("scriptSha".getBytes(), ReturnType.BOOLEAN, 1, "keysAndArgs".getBytes());
  }

  @Test
  public void invokingGeoAddCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.GEOADD, () -> getConnection().geoAdd("key".getBytes(), null,
        "member".getBytes()));
    verify(mockRedisConnection()).geoAdd("key".getBytes(), null, "member".getBytes());

    commandCreatesNewSpan(RedisCommand.GEOADD, () -> getConnection().geoAdd("key".getBytes(),
        (RedisGeoCommands.GeoLocation<byte[]>) null));
    verify(mockRedisConnection())
        .geoAdd("key".getBytes(), (RedisGeoCommands.GeoLocation<byte[]>) null);

    commandCreatesNewSpan(RedisCommand.GEOADD,
        () -> getConnection().geoAdd("key".getBytes(), (Map<byte[],
            Point>) null));
    verify(mockRedisConnection()).geoAdd("key".getBytes(), (Map<byte[], Point>) null);

    commandCreatesNewSpan(RedisCommand.GEOADD, () -> getConnection().geoAdd("key".getBytes(),
        (Iterable<RedisGeoCommands.GeoLocation<byte[]>>) null));
    verify(mockRedisConnection())
        .geoAdd("key".getBytes(), (Iterable<RedisGeoCommands.GeoLocation<byte[]>>) null);
  }

  @Test
  public void invokingGeoDistCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.GEODIST, () -> getConnection().geoDist("key".getBytes(),
        "member1".getBytes(), "member2".getBytes()));
    verify(mockRedisConnection())
        .geoDist("key".getBytes(), "member1".getBytes(), "member2".getBytes());

    commandCreatesNewSpan(RedisCommand.GEODIST, () -> getConnection().geoDist("key".getBytes(),
        "member1".getBytes(), "member2".getBytes(), null));
    verify(mockRedisConnection())
        .geoDist("key".getBytes(), "member1".getBytes(), "member2".getBytes(), null);
  }

  @Test
  public void invokingGeoHashCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.GEOHASH,
        () -> getConnection().geoHash("key".getBytes(), "member".getBytes()));
    verify(mockRedisConnection()).geoHash("key".getBytes(), "member".getBytes());
  }

  @Test
  public void invokingGeoPosCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.GEOPOS,
        () -> getConnection().geoPos("key".getBytes(), "member".getBytes()));
    verify(mockRedisConnection()).geoPos("key".getBytes(), "member".getBytes());
  }

  @Test
  public void invokingGeoRadiusCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.GEORADIUS,
        () -> getConnection().geoRadius("key".getBytes(), null));
    verify(mockRedisConnection()).geoRadius("key".getBytes(), null);

    commandCreatesNewSpan(RedisCommand.GEORADIUS,
        () -> getConnection().geoRadius("key".getBytes(), null,
            RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()));
    verify(mockRedisConnection()).geoRadius(eq("key".getBytes()), isNull(),
        any(RedisGeoCommands.GeoRadiusCommandArgs.class));
  }

  @Test
  public void invokingGeoRadiusByMemberCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.GEORADIUSBYMEMBER,
        () -> getConnection().geoRadiusByMember("key".getBytes(), "member".getBytes(), 0D));
    verify(mockRedisConnection()).geoRadiusByMember("key".getBytes(), "member".getBytes(), 0D);

    commandCreatesNewSpan(RedisCommand.GEORADIUSBYMEMBER,
        () -> getConnection().geoRadiusByMember("key".getBytes(), "member".getBytes(), null));
    verify(mockRedisConnection()).geoRadiusByMember("key".getBytes(), "member".getBytes(), null);

    commandCreatesNewSpan(RedisCommand.GEORADIUSBYMEMBER,
        () -> getConnection().geoRadiusByMember("key".getBytes(), "member".getBytes(), null,
            RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()));
    verify(mockRedisConnection())
        .geoRadiusByMember(eq("key".getBytes()), eq("member".getBytes()), isNull(),
            any(RedisGeoCommands.GeoRadiusCommandArgs.class));
  }

  @Test
  public void invokingGeoRemoveCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.GEOREMOVE, () -> getConnection().geoRemove("key".getBytes(),
        "member".getBytes()));
    verify(mockRedisConnection()).geoRemove("key".getBytes(), "member".getBytes());
  }

  @Test
  public void invokingPfAddCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.PFADD,
        () -> getConnection().pfAdd("key".getBytes(), "val".getBytes()));
    verify(mockRedisConnection()).pfAdd("key".getBytes(), "val".getBytes());
  }

  @Test
  public void invokingPfCountCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.PFCOUNT, () -> getConnection().pfCount("key".getBytes()));
    verify(mockRedisConnection()).pfCount("key".getBytes());
  }

  @Test
  public void invokingPfMergeCreatesNewSpan() {
    commandCreatesNewSpan(RedisCommand.PFMERGE,
        () -> getConnection().pfMerge("dst".getBytes(), "src".getBytes()));
    verify(mockRedisConnection()).pfMerge("dst".getBytes(), "src".getBytes());
  }

  /*
   * The test below verify the creation of a child Span. We only do it for one invocation as they all use the same code.
   */

  @Test
  public void spanJoinsActiveSpan() {
    commandSpanJoinsActiveSpan(() -> getConnection().execute("command"));
  }

}

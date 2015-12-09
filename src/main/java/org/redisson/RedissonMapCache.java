/**
 * Copyright 2014 Nikita Koksharov, Nickolay Borbit
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.redisson.client.codec.Codec;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommand.ValueType;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.convertor.BooleanReplayConvertor;
import org.redisson.client.protocol.convertor.Convertor;
import org.redisson.client.protocol.convertor.LongReplayConvertor;
import org.redisson.client.protocol.decoder.MapScanResult;
import org.redisson.client.protocol.decoder.MapScanResultReplayDecoder;
import org.redisson.client.protocol.decoder.NestedMultiDecoder;
import org.redisson.client.protocol.decoder.ObjectListReplayDecoder;
import org.redisson.client.protocol.decoder.ObjectMapReplayDecoder;
import org.redisson.client.protocol.decoder.TTLMapValueReplayDecoder;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.connection.decoder.CacheGetAllDecoder;
import org.redisson.core.RMapCache;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

/**
 * <p>Map-based cache with ability to set TTL for each entry via
 * {@link #put(Object, Object, long, TimeUnit)} or {@link #putIfAbsent(Object, Object, long, TimeUnit)}
 * And therefore has an complex lua-scripts inside.</p>
 *
 * <p>Current redis implementation doesnt have eviction functionality.
 * Thus entries are checked for TTL expiration during any key/value/entry read operation.
 * If key/value/entry expired then it doesn't returns and clean task runs asynchronous.
 * Clean task deletes removes 100 expired entries at once.
 * In addition there is {@link org.redisson.RedissonCacheEvictionScheduler}. This scheduler
 * deletes expired entries in time interval between 5 seconds to 2 hours.</p>
 *
 * <p>If eviction is not required then it's better to use {@link org.redisson.reactive.RedissonMapReactive}.</p>
 *
 * @author Nikita Koksharov
 *
 * @param <K> key
 * @param <V> value
 */
public class RedissonMapCache<K, V> extends RedissonMap<K, V> implements RMapCache<K, V> {

    private static final RedisCommand<MapScanResult<Object, Object>> EVAL_HSCAN = new RedisCommand<MapScanResult<Object, Object>>("EVAL", new NestedMultiDecoder(new ObjectMapReplayDecoder(), new MapScanResultReplayDecoder()), ValueType.MAP);
    private static final RedisCommand<Object> EVAL_REMOVE = new RedisCommand<Object>("EVAL", 4, ValueType.MAP_KEY, ValueType.MAP_VALUE);
    private static final RedisCommand<Long> EVAL_REMOVE_VALUE = new RedisCommand<Long>("EVAL", new LongReplayConvertor(), 5, ValueType.MAP);
    private static final RedisCommand<Object> EVAL_PUT_TTL = new RedisCommand<Object>("EVAL", 6, ValueType.MAP, ValueType.MAP_VALUE);
    private static final RedisCommand<List<Object>> EVAL_GET_TTL = new RedisCommand<List<Object>>("EVAL", new TTLMapValueReplayDecoder<Object>(), 5, ValueType.MAP_KEY, ValueType.MAP_VALUE);
    private static final RedisCommand<List<Object>> EVAL_CONTAINS_KEY = new RedisCommand<List<Object>>("EVAL", new ObjectListReplayDecoder<Object>(), 5, ValueType.MAP_KEY);
    private static final RedisCommand<List<Object>> EVAL_CONTAINS_VALUE = new RedisCommand<List<Object>>("EVAL", new ObjectListReplayDecoder<Object>(), 5, ValueType.MAP_VALUE);
    private static final RedisCommand<Long> EVAL_FAST_REMOVE = new RedisCommand<Long>("EVAL", 5, ValueType.MAP_KEY);
    private static final RedisCommand<Long> EVAL_REMOVE_EXPIRED = new RedisCommand<Long>("EVAL", 5);

    private static final RedissonCacheEvictionScheduler SCHEDULER = new RedissonCacheEvictionScheduler();

    protected RedissonMapCache(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        SCHEDULER.schedule(getName(), getTimeoutSetName(), commandExecutor);
    }

    public RedissonMapCache(Codec codec, CommandAsyncExecutor commandExecutor, String name) {
        super(codec, commandExecutor, name);
        SCHEDULER.schedule(getName(), getTimeoutSetName(), commandExecutor);
    }

    @Override
    public Future<Boolean> containsKeyAsync(Object key) {
        Promise<Boolean> result = newPromise();

        Future<List<Object>> future = commandExecutor.evalReadAsync(getName(), codec, EVAL_CONTAINS_KEY,
                "local value = redis.call('hexists', KEYS[1], ARGV[1]); " +
                "local expireDate = 92233720368547758; " +
                "if value == 1 then " +
                    "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[1]); "
                    + "if expireDateScore ~= false then "
                        + "expireDate = tonumber(expireDateScore) "
                    + "end; " +
                "end;" +
                "return {expireDate, value}; ",
               Arrays.<Object>asList(getName(), getTimeoutSetName()), key);

        addExpireListener(result, future, new BooleanReplayConvertor(), false);

        return result;
    }

    @Override
    public Future<Boolean> containsValueAsync(Object value) {
        Promise<Boolean> result = newPromise();

        Future<List<Object>> future = commandExecutor.evalReadAsync(getName(), codec, EVAL_CONTAINS_VALUE,
                        "local s = redis.call('hgetall', KEYS[1]);" +
                        "for i, v in ipairs(s) do "
                            + "if i % 2 == 0 and ARGV[1] == v then "
                                + "local key = s[i-1];"
                                + "local expireDate = redis.call('zscore', KEYS[2], key); "
                                + "if expireDate == false then "
                                    + "expireDate = 92233720368547758 "
                                + "else "
                                    + "expireDate = tonumber(expireDate) "
                                + "end; "
                                + "return {expireDate, 1}; "
                            + "end "
                       + "end;" +
                     "return {92233720368547758, 0};",
                 Arrays.<Object>asList(getName(), getTimeoutSetName()), value);

        addExpireListener(result, future, new BooleanReplayConvertor(), false);

        return result;
    }

    @Override
    public Future<Map<K, V>> getAllAsync(Set<K> keys) {
        if (keys.isEmpty()) {
            return newSucceededFuture(Collections.<K, V>emptyMap());
        }

        List<Object> args = new ArrayList<Object>(keys.size() + 2);
        args.add(System.currentTimeMillis());
        args.addAll(keys);

        final Promise<Map<K, V>> result = newPromise();
        Future<List<Object>> future = commandExecutor.evalReadAsync(getName(), codec, new RedisCommand<List<Object>>("EVAL", new CacheGetAllDecoder(args), 6, ValueType.MAP_KEY, ValueType.MAP_VALUE),
                        "local expireSize = redis.call('zcard', KEYS[2]); " +
                        "local maxDate = table.remove(ARGV, 1); " // index is the first parameter
                        + "local minExpireDate = 92233720368547758;" +
                        "if expireSize > 0 then "
                        + "for i, key in pairs(ARGV) do "
                            + "local expireDate = redis.call('zscore', KEYS[2], key); "
                            + "if expireDate ~= false and expireDate <= maxDate then "
                                + "minExpireDate = math.min(tonumber(expireDate), minExpireDate); "
                                + "ARGV[i] = ARGV[i] .. '__redisson__skip' "
                            + "end;"
                        + "end;"
                      + "end; " +
                       "return {minExpireDate, unpack(redis.call('hmget', KEYS[1], unpack(ARGV)))};",
                Arrays.<Object>asList(getName(), getTimeoutSetName()), args.toArray());

        future.addListener(new FutureListener<List<Object>>() {
            @Override
            public void operationComplete(Future<List<Object>> future) throws Exception {
                if (!future.isSuccess()) {
                    result.setFailure(future.cause());
                    return;
                }

                List<Object> res = future.getNow();
                Long expireDate = (Long) res.get(0);
                long currentDate = System.currentTimeMillis();
                if (expireDate <= currentDate) {
                    expireMap(currentDate);
                }

                result.setSuccess((Map<K, V>) res.get(1));
            }
        });

        return result;

    }

    @Override
    public V putIfAbsent(K key, V value, long ttl, TimeUnit unit) {
        return get(putIfAbsentAsync(key, value, ttl, unit));
    }

    @Override
    public Future<V> putIfAbsentAsync(K key, V value, long ttl, TimeUnit unit) {
        if (unit == null) {
            throw new NullPointerException("TimeUnit param can't be null");
        }

        long timeoutDate = System.currentTimeMillis() + unit.toMillis(ttl);
        return commandExecutor.evalWriteAsync(getName(), codec, EVAL_PUT_TTL,
                "if redis.call('hexists', KEYS[1], ARGV[2]) == 0 then "
                        + "redis.call('zadd', KEYS[2], ARGV[1], ARGV[2]); "
                        + "redis.call('hset', KEYS[1], ARGV[2], ARGV[3]); "
                        + "return nil "
                    + "else "
                        + "return redis.call('hget', KEYS[1], ARGV[2]) "
                    + "end",
                Arrays.<Object>asList(getName(), getTimeoutSetName()), timeoutDate, key, value);
    }

    @Override
    public Future<Long> removeAsync(Object key, Object value) {
        return commandExecutor.evalWriteAsync(getName(), codec, EVAL_REMOVE_VALUE,
                "if redis.call('hget', KEYS[1], ARGV[1]) == ARGV[2] then "
                        + "redis.call('zrem', KEYS[2], ARGV[1]); "
                        + "return redis.call('hdel', KEYS[1], ARGV[1]); "
                + "else "
                    + "return 0 "
                + "end",
                Arrays.<Object>asList(getName(), getTimeoutSetName()), key, value);
    }

    @Override
    public Future<V> getAsync(K key) {
        Promise<V> result = newPromise();

        Future<List<Object>> future = commandExecutor.evalReadAsync(getName(), codec, EVAL_GET_TTL,
                 "local value = redis.call('hget', KEYS[1], ARGV[1]); " +
                 "local expireDate = redis.call('zscore', KEYS[2], ARGV[1]); "
                 + "if expireDate == false then "
                     + "expireDate = 92233720368547758; "
                 + "end; " +
                 "return {expireDate, value}; ",
                Arrays.<Object>asList(getName(), getTimeoutSetName()), key);

        addExpireListener(result, future, null, null);

        return result;
    }

    private <T> void addExpireListener(final Promise<T> result, Future<List<Object>> future, final Convertor<T> convertor, final T nullValue) {
        future.addListener(new FutureListener<List<Object>>() {
            @Override
            public void operationComplete(Future<List<Object>> future) throws Exception {
                if (!future.isSuccess()) {
                    result.setFailure(future.cause());
                    return;
                }

                List<Object> res = future.getNow();
                Long expireDate = (Long) res.get(0);
                long currentDate = System.currentTimeMillis();
                if (expireDate <= currentDate) {
                    result.setSuccess(nullValue);
                    expireMap(currentDate);
                    return;
                }

                if (convertor != null) {
                    result.setSuccess((T) convertor.convert(res.get(1)));
                } else {
                    result.setSuccess((T) res.get(1));
                }
            }
        });
    }

    private void expireMap(long currentDate) {
        commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, EVAL_REMOVE_EXPIRED,
                "local expiredKeys = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1], 'limit', 0, 100); "
                        + "if #expiredKeys > 0 then "
                            + "local s = redis.call('zrem', KEYS[2], unpack(expiredKeys)); "
                            + "redis.call('hdel', KEYS[1], unpack(expiredKeys)); "
                        + "end;",
                        Arrays.<Object>asList(getName(), getTimeoutSetName()), currentDate);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit unit) {
        return get(putAsync(key, value, ttl, unit));
    }

    @Override
    public Future<V> putAsync(K key, V value, long ttl, TimeUnit unit) {
        if (unit == null) {
            throw new NullPointerException("TimeUnit param can't be null");
        }

        long timeoutDate = System.currentTimeMillis() + unit.toMillis(ttl);
        return commandExecutor.evalWriteAsync(getName(), codec, EVAL_PUT_TTL,
                "local v = redis.call('hget', KEYS[1], ARGV[2]); "
                + "redis.call('zadd', KEYS[2], ARGV[1], ARGV[2]); "
                + "redis.call('hset', KEYS[1], ARGV[2], ARGV[3]); "
                + "return v",
                Arrays.<Object>asList(getName(), getTimeoutSetName()), timeoutDate, key, value);
    }

    String getTimeoutSetName() {
        return "redisson__timeout__set__{" + getName() + "}";
    }


    @Override
    public Future<V> removeAsync(K key) {
        return commandExecutor.evalWriteAsync(getName(), codec, EVAL_REMOVE,
                "local v = redis.call('hget', KEYS[1], ARGV[1]); "
                + "redis.call('zrem', KEYS[2], ARGV[1]); "
                + "redis.call('hdel', KEYS[1], ARGV[1]); "
                + "return v",
                Arrays.<Object>asList(getName(), getTimeoutSetName()), key);
    }

    @Override
    public Future<Long> fastRemoveAsync(K ... keys) {
        if (keys == null || keys.length == 0) {
            return newSucceededFuture(0L);
        }

        return commandExecutor.evalWriteAsync(getName(), codec, EVAL_FAST_REMOVE,
                "redis.call('zrem', KEYS[2], unpack(ARGV)); "
                + "return redis.call('hdel', KEYS[1], unpack(ARGV)); ",
                Arrays.<Object>asList(getName(), getTimeoutSetName()), keys);
    }

    @Override
    MapScanResult<Object, V> scanIterator(InetSocketAddress client, long startPos) {
        Future<MapScanResult<Object, V>> f = commandExecutor.evalReadAsync(client, getName(), codec, EVAL_HSCAN,
                "local result = {}; "
                + "local res = redis.call('hscan', KEYS[1], ARGV[1]); "
                + "for i, value in ipairs(res[2]) do "
                    + "if i % 2 == 0 then "
                        + "local key = res[2][i-1]; "
                        + "local expireDate = redis.call('zscore', KEYS[2], key); "
                        + "if (expireDate == false) or (expireDate ~= false and expireDate > ARGV[2]) then "
                            + "table.insert(result, key); "
                            + "table.insert(result, value); "
                        + "end; "
                    + "end; "
                + "end;"
                + "return {res[1], result};", Arrays.<Object>asList(getName(), getTimeoutSetName()), startPos, System.currentTimeMillis());

        return get(f);
    }

    @Override
    public Future<Boolean> deleteAsync() {
        return commandExecutor.writeAsync(getName(), RedisCommands.DEL_SINGLE, getName(), getTimeoutSetName());
    }

    @Override
    public Future<Boolean> expireAsync(long timeToLive, TimeUnit timeUnit) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "redis.call('pexpire', KEYS[2], ARGV[1]); "
                + "return redis.call('pexpire', KEYS[1], ARGV[1]); ",
                Arrays.<Object>asList(getName(), getTimeoutSetName()), timeUnit.toSeconds(timeToLive));
    }

    @Override
    public Future<Boolean> expireAtAsync(long timestamp) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "redis.call('pexpireat', KEYS[2], ARGV[1]); "
                + "return redis.call('pexpireat', KEYS[1], ARGV[1]); ",
                Arrays.<Object>asList(getName(), getTimeoutSetName()), timestamp);
    }

    @Override
    public Future<Boolean> clearExpireAsync() {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "redis.call('persist', KEYS[2]); "
                + "return redis.call('persist', KEYS[1]); ",
                Arrays.<Object>asList(getName(), getTimeoutSetName()));
    }

}

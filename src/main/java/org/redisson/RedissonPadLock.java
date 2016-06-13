/**
 * Copyright 2014 Nikita Koksharov, Nickolay Borbit
 * Copyright 2016 Alexander Shulgin
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
/**
 * This class based on knowledge and previous code that was provided in 
 * {@link org.redisson.RedissonLock} hence previous authors included.
 */
package org.redisson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.redisson.client.RedisException;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandExecutor;
import org.redisson.core.RPadLock;
import org.redisson.pubsub.LockPubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.PlatformDependent;

/**
 * Default implementation of {@link RPadLock} {@see RPadLock}
 * 
 * @author Nikita Koksharov, Nickolay Borbit, Alexander Shulgin
 *
 */
public class RedissonPadLock extends RedissonExpirable implements RPadLock {

    private final Logger log = LoggerFactory.getLogger(RedissonPadLock.class);

    public static final long LOCK_EXPIRATION_INTERVAL_SECONDS = 30;
    private static final ConcurrentMap<String, Timeout> expirationRenewalMap = PlatformDependent.newConcurrentHashMap();
    protected long internalLockLeaseTime = TimeUnit.SECONDS.toMillis(LOCK_EXPIRATION_INTERVAL_SECONDS);

    final UUID id;

    protected static final LockPubSub PUBSUB = new LockPubSub();

    final CommandExecutor commandExecutor;

    protected RedissonPadLock(CommandExecutor commandExecutor, String name, UUID id) {
	super(commandExecutor, name);
	this.commandExecutor = commandExecutor;
	this.id = id;
    }

    protected String getEntryName() {
	return id + ":" + getName();
    }

    String getChannelName() {
	return "redisson_lock__channel__{" + getName() + "}";
    }

    @Override
    public void lock(String[] ownerKeys) {
	try {
	    lockInterruptibly(ownerKeys);
	} catch (InterruptedException e) {
	    Thread.currentThread().interrupt();
	}
    }

    @Override
    public void lock(String[] ownerKeys, long leaseTime, TimeUnit unit) {
	try {
	    lockInterruptibly(ownerKeys, leaseTime, unit);
	} catch (InterruptedException e) {
	    Thread.currentThread().interrupt();
	}
    }

    @Override
    public void lockInterruptibly(String[] ownerKeys) throws InterruptedException {
	lockInterruptibly(ownerKeys, -1, null);
    }

    @Override
    public void lockInterruptibly(String[] ownerKeys, long leaseTime, TimeUnit unit) throws InterruptedException {
	Long ttl = tryAcquire(ownerKeys, leaseTime, unit);
	// lock acquired
	if (ttl == null) {
	    return;
	}

	Future<RedissonLockEntry> future = subscribe();
	get(future);

	try {
	    while (true) {
		ttl = tryAcquire(ownerKeys, leaseTime, unit);
		// lock acquired
		if (ttl == null) {
		    break;
		}

		// waiting for message
		if (ttl >= 0) {
		    getEntry().getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
		} else {
		    getEntry().getLatch().acquire();
		}
	    }
	} finally {
	    unsubscribe(future);
	}
    }

    private Long tryAcquire(String[] ownerKeys, long leaseTime, TimeUnit unit) {
	return get(tryAcquireAsync(leaseTime, unit, ownerKeys));
    }

    private Future<Boolean> tryAcquireOnceAsync(long leaseTime, TimeUnit unit, String[] ownerKeys) {
	if (leaseTime != -1) {
	    return tryLockInnerAsync(leaseTime, unit, ownerKeys, RedisCommands.EVAL_NULL_BOOLEAN);
	}
	Future<Boolean> ttlRemainingFuture = tryLockInnerAsync(LOCK_EXPIRATION_INTERVAL_SECONDS, TimeUnit.SECONDS,
		ownerKeys, RedisCommands.EVAL_NULL_BOOLEAN);
	ttlRemainingFuture.addListener(new FutureListener<Boolean>() {
	    @Override
	    public void operationComplete(Future<Boolean> future) throws Exception {
		if (!future.isSuccess()) {
		    return;
		}

		Boolean ttlRemaining = future.getNow();
		// lock acquired
		if (ttlRemaining) {
		    scheduleExpirationRenewal();
		}
	    }
	});
	return ttlRemainingFuture;
    }

    private <T> Future<Long> tryAcquireAsync(long leaseTime, TimeUnit unit, String[] ownerKeys) {
	if (leaseTime != -1) {
	    return tryLockInnerAsync(leaseTime, unit, ownerKeys, RedisCommands.EVAL_LONG);
	}
	Future<Long> ttlRemainingFuture = tryLockInnerAsync(LOCK_EXPIRATION_INTERVAL_SECONDS, TimeUnit.SECONDS,
		ownerKeys, RedisCommands.EVAL_LONG);
	ttlRemainingFuture.addListener(new FutureListener<Long>() {
	    @Override
	    public void operationComplete(Future<Long> future) throws Exception {
		if (!future.isSuccess()) {
		    return;
		}

		Long ttlRemaining = future.getNow();
		// lock acquired
		if (ttlRemaining == null) {
		    scheduleExpirationRenewal();
		}
	    }
	});
	return ttlRemainingFuture;
    }

    @Override
    public boolean tryLock(String[] ownerKeys) {
	return get(tryLockAsync(ownerKeys));
    }

    private void scheduleExpirationRenewal() {
	if (expirationRenewalMap.containsKey(getEntryName())) {
	    return;
	}

	Timeout task = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
	    @Override
	    public void run(Timeout timeout) throws Exception {
		Future<Boolean> future = expireAsync(internalLockLeaseTime, TimeUnit.MILLISECONDS);
		future.addListener(new FutureListener<Boolean>() {
		    @Override
		    public void operationComplete(Future<Boolean> future) throws Exception {
			expirationRenewalMap.remove(getEntryName());
			if (!future.isSuccess()) {
			    log.error("Can't update lock " + getName() + " expiration", future.cause());
			    return;
			}

			if (future.getNow()) {
			    // reschedule itself
			    scheduleExpirationRenewal();
			}
		    }
		});
	    }
	}, internalLockLeaseTime / 3, TimeUnit.MILLISECONDS);

	if (expirationRenewalMap.putIfAbsent(getEntryName(), task) != null) {
	    task.cancel();
	}
    }

    protected void cancelExpirationRenewal() {
	Timeout task = expirationRenewalMap.remove(getEntryName());
	if (task != null) {
	    task.cancel();
	}
    }

    protected <T> Future<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, String[] ownerKeys,
	    RedisStrictCommand<T> command) {
	internalLockLeaseTime = unit.toMillis(leaseTime);
	final int keyArgIdxOffset = 2;
	StringBuilder script = new StringBuilder();
	script.append("if (redis.call('exists', KEYS[1]) == 0) then ");
	for (int i = 0; i < ownerKeys.length; i++) {
	    script.append("redis.call('hset', KEYS[1], ARGV[").append(i + keyArgIdxOffset).append("], 1); ");
	}
	script.append("redis.call('pexpire', KEYS[1], ARGV[1]); ");
	script.append("return nil; ");
	script.append("end; ");

	script.append("if (");
	for (int i = 0; i < ownerKeys.length; i++) {
	    if (i > 0) {
		script.append("or ");
	    }
	    script.append("redis.call('hexists', KEYS[1], ARGV[").append(i + keyArgIdxOffset).append("]) == 1 ");
	}
	script.append(") then ");
	for (int i = 0; i < ownerKeys.length; i++) {
	    script.append("redis.call('hincrby', KEYS[1], ARGV[").append(i + keyArgIdxOffset).append("], 1); ");
	}
	script.append("redis.call('pexpire', KEYS[1], ARGV[1]); ");
	script.append("return nil; ");
	script.append("end; ");

	script.append("return redis.call('pttl', KEYS[1]); ");

	List<String> params = new ArrayList<>();
	params.add(String.valueOf(internalLockLeaseTime));
	params.addAll(Arrays.asList(ownerKeys));
	return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command, script.toString(),
		Collections.<Object> singletonList(getName()), params.toArray());
    }

    @Override
    public boolean tryLock(String[] ownerKeys, long waitTime, long leaseTime, TimeUnit unit)
	    throws InterruptedException {
	long time = unit.toMillis(waitTime);
	Long ttl = tryAcquire(ownerKeys, leaseTime, unit);
	// lock acquired
	if (ttl == null) {
	    return true;
	}

	Future<RedissonLockEntry> future = subscribe();
	if (!await(future, time, TimeUnit.MILLISECONDS)) {
	    future.addListener(new FutureListener<RedissonLockEntry>() {
		@Override
		public void operationComplete(Future<RedissonLockEntry> future) throws Exception {
		    if (future.isSuccess()) {
			unsubscribe(future);
		    }
		}
	    });
	    return false;
	}

	try {
	    while (true) {
		ttl = tryAcquire(ownerKeys, leaseTime, unit);
		// lock acquired
		if (ttl == null) {
		    return true;
		}

		if (time <= 0) {
		    return false;
		}

		// waiting for message
		long current = System.currentTimeMillis();
		if (ttl >= 0 && ttl < time) {
		    getEntry().getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
		} else {
		    getEntry().getLatch().tryAcquire(time, TimeUnit.MILLISECONDS);
		}

		long elapsed = System.currentTimeMillis() - current;
		time -= elapsed;
	    }
	} finally {
	    unsubscribe(future);
	}
    }

    protected RedissonLockEntry getEntry() {
	return PUBSUB.getEntry(getEntryName());
    }

    protected Future<RedissonLockEntry> subscribe() {
	return PUBSUB.subscribe(getEntryName(), getChannelName(), commandExecutor.getConnectionManager());
    }

    protected void unsubscribe(Future<RedissonLockEntry> future) {
	PUBSUB.unsubscribe(future.getNow(), getEntryName(), getChannelName(), commandExecutor.getConnectionManager());
    }

    @Override
    public boolean tryLock(String[] ownerKeys, long waitTime, TimeUnit unit) throws InterruptedException {
	return tryLock(ownerKeys, waitTime, -1, unit);
    }

    @Override
    public void unlock(String[] ownerKeys) {
	try {
	    get(unlockAsync(ownerKeys));
	} catch (RedisException re) {
	    if (re.getCause() != null && re.getCause() instanceof IllegalMonitorStateException) {
		throw (IllegalMonitorStateException) re.getCause();
	    } else {
		throw re;
	    }
	}
    }

    @Override
    public void forceUnlock() {
	get(forceUnlockAsync());
    }

    public Future<Boolean> forceUnlockAsync() {
	cancelExpirationRenewal();
	StringBuilder script = new StringBuilder();
	script.append("if (redis.call('del', KEYS[1]) == 1) then ");
	script.append("redis.call('del', KEYS[1]); ");
	script.append("redis.call('publish', KEYS[2], ARGV[1]); ");
	script.append("return 1; ");
	script.append("else ");
	script.append("return 0; ");
	script.append("end; ");

	return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
		script.toString(), Arrays.<Object> asList(getName(), getChannelName()), LockPubSub.unlockMessage);
    }

    @Override
    public boolean isLocked() {
	return isExists();
    }

    @Override
    public boolean isOwner(String ownerKey) {
	return commandExecutor.read(getName(), LongCodec.INSTANCE, RedisCommands.HEXISTS, getName(), ownerKey);
    }

    @Override
    public int getHoldCount(String ownerKey) {
	Long res = commandExecutor.read(getName(), LongCodec.INSTANCE, RedisCommands.HGET, getName(), ownerKey);
	if (res == null) {
	    return 0;
	}
	return res.intValue();
    }

    @Override
    public Future<Boolean> deleteAsync() {
	return forceUnlockAsync();
    }

    public Future<Void> unlockAsync(final String[] ownerKeys) {
	final int keyArgIdxOffset = 3;
	StringBuilder script = new StringBuilder();
	script.append("if (redis.call('exists', KEYS[1]) == 0) then ");
	script.append("redis.call('publish', KEYS[2], ARGV[1]); ");
	script.append("return 1; ");
	script.append("end; ");
	script.append("if (");
	for (int i = 0; i < ownerKeys.length; i++) {
	    if (i > 0) {
		script.append("and ");
	    }
	    script.append("redis.call('hexists', KEYS[1], ARGV[").append(i + keyArgIdxOffset).append("]) == 0 ");
	}
	script.append(") then ");
	script.append("return nil; ");
	script.append("end; ");
	script.append("local counter = 0; ");
	for (int i = 0; i < ownerKeys.length; i++) {
	    script.append("if (redis.call('hexists', KEYS[1], ARGV[").append(i + keyArgIdxOffset)
		    .append("]) == 1) then ");
	    script.append("if (redis.call('hincrby', KEYS[1], ARGV[").append(i + keyArgIdxOffset)
		    .append("], -1) > 0) then ");
	    script.append("counter = counter + 1; ");
	    script.append("end; ");
	    script.append("end; ");
	}
	script.append("if (counter > 0) then ");
	script.append("redis.call('pexpire', KEYS[1], ARGV[2]); ");
	script.append("return 0; ");
	script.append("else ");
	script.append("redis.call('del', KEYS[1]); ");
	script.append("redis.call('publish', KEYS[2], ARGV[1]); ");
	script.append("return 1; ");
	script.append("end; ");
	script.append("return nil; ");

	List<String> params = new ArrayList<>();
	params.add(String.valueOf(LockPubSub.unlockMessage));
	params.add(String.valueOf(internalLockLeaseTime));
	params.addAll(Arrays.asList(ownerKeys));

	final Promise<Void> result = newPromise();
	Future<Boolean> future = commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE,
		RedisCommands.EVAL_BOOLEAN, script.toString(), Arrays.<Object> asList(getName(), getChannelName()),
		params.toArray());

	future.addListener(new FutureListener<Boolean>() {
	    @Override
	    public void operationComplete(Future<Boolean> future) throws Exception {
		if (!future.isSuccess()) {
		    result.setFailure(future.cause());
		    return;
		}

		Boolean opStatus = future.getNow();
		if (opStatus == null) {
		    IllegalMonitorStateException cause = new IllegalMonitorStateException(
			    "attempt to unlock lock, not locked by current owner by node id: " + id + " owner: "
				    + ownerKeys);
		    result.setFailure(cause);
		    return;
		}
		if (opStatus) {
		    cancelExpirationRenewal();
		}
		result.setSuccess(null);
	    }
	});

	return result;
    }

    public Future<Void> lockAsync(String[] ownerKeys) {
	return lockAsync(ownerKeys, -1, null);
    }

    @Override
    public Future<Void> lockAsync(String[] ownerKeys, long leaseTime, TimeUnit unit) {
	final Promise<Void> result = newPromise();
	Future<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, ownerKeys);
	ttlFuture.addListener(new FutureListener<Long>() {
	    @Override
	    public void operationComplete(Future<Long> future) throws Exception {
		if (!future.isSuccess()) {
		    result.setFailure(future.cause());
		    return;
		}

		Long ttl = future.getNow();

		// lock acquired
		if (ttl == null) {
		    result.setSuccess(null);
		    return;
		}

		final Future<RedissonLockEntry> subscribeFuture = subscribe();
		subscribeFuture.addListener(new FutureListener<RedissonLockEntry>() {
		    @Override
		    public void operationComplete(Future<RedissonLockEntry> future) throws Exception {
			if (!future.isSuccess()) {
			    result.setFailure(future.cause());
			    return;
			}

			lockAsync(leaseTime, unit, subscribeFuture, result, ownerKeys);
		    }

		});
	    }
	});

	return result;
    }

    private void lockAsync(final long leaseTime, final TimeUnit unit, final Future<RedissonLockEntry> subscribeFuture,
	    final Promise<Void> result, final String[] ownerKeys) {
	Future<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, ownerKeys);
	ttlFuture.addListener(new FutureListener<Long>() {
	    @Override
	    public void operationComplete(Future<Long> future) throws Exception {
		if (!future.isSuccess()) {
		    unsubscribe(subscribeFuture);
		    result.setFailure(future.cause());
		    return;
		}

		Long ttl = future.getNow();
		// lock acquired
		if (ttl == null) {
		    unsubscribe(subscribeFuture);
		    result.setSuccess(null);
		    return;
		}

		// waiting for message
		final RedissonLockEntry entry = getEntry();
		synchronized (entry) {
		    if (entry.getLatch().tryAcquire()) {
			lockAsync(leaseTime, unit, subscribeFuture, result, ownerKeys);
		    } else {
			final AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<ScheduledFuture<?>>();
			final Runnable listener = new Runnable() {
			    @Override
			    public void run() {
				if (futureRef.get() != null) {
				    futureRef.get().cancel(false);
				}
				lockAsync(leaseTime, unit, subscribeFuture, result, ownerKeys);
			    }
			};

			entry.addListener(listener);

			if (ttl >= 0) {
			    ScheduledFuture<?> scheduledFuture = commandExecutor.getConnectionManager().getGroup()
				    .schedule(new Runnable() {
					@Override
					public void run() {
					    synchronized (entry) {
						if (entry.removeListener(listener)) {
						    lockAsync(leaseTime, unit, subscribeFuture, result, ownerKeys);
						}
					    }
					}
				    }, ttl, TimeUnit.MILLISECONDS);
			    futureRef.set(scheduledFuture);
			}
		    }
		}
	    }
	});
    }

    public Future<Boolean> tryLockAsync(String[] ownerKeys) {
	return tryAcquireOnceAsync(-1, null, ownerKeys);
    }

    public Future<Boolean> tryLockAsync(String[] ownerKeys, long waitTime, TimeUnit unit) {
	return tryLockAsync(ownerKeys, waitTime, -1, unit);
    }

    public Future<Boolean> tryLockAsync(String[] ownerKeys, long waitTime, long leaseTime, TimeUnit unit) {
	return tryLockAsync(waitTime, leaseTime, unit, ownerKeys);
    }

    public Future<Boolean> tryLockAsync(final long waitTime, final long leaseTime, final TimeUnit unit,
	    final String[] ownerKeys) {
	final Promise<Boolean> result = newPromise();

	final AtomicLong time = new AtomicLong(unit.toMillis(waitTime));
	Future<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, ownerKeys);
	ttlFuture.addListener(new FutureListener<Long>() {
	    @Override
	    public void operationComplete(Future<Long> future) throws Exception {
		if (!future.isSuccess()) {
		    result.setFailure(future.cause());
		    return;
		}

		Long ttl = future.getNow();

		// lock acquired
		if (ttl == null) {
		    result.setSuccess(true);
		    return;
		}

		final long current = System.currentTimeMillis();
		final AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<ScheduledFuture<?>>();
		final Future<RedissonLockEntry> subscribeFuture = subscribe();
		subscribeFuture.addListener(new FutureListener<RedissonLockEntry>() {
		    @Override
		    public void operationComplete(Future<RedissonLockEntry> future) throws Exception {
			if (!future.isSuccess()) {
			    result.tryFailure(future.cause());
			    return;
			}

			if (futureRef.get() != null) {
			    futureRef.get().cancel(false);
			}

			long elapsed = System.currentTimeMillis() - current;
			time.addAndGet(-elapsed);

			if (time.get() < 0) {
			    unsubscribe(subscribeFuture);
			    result.trySuccess(false);
			    return;
			}

			tryLockAsync(time, leaseTime, unit, subscribeFuture, result, ownerKeys);
		    }
		});
		if (!subscribeFuture.isDone()) {
		    ScheduledFuture<?> scheduledFuture = commandExecutor.getConnectionManager().getGroup()
			    .schedule(new Runnable() {
				@Override
				public void run() {
				    if (!subscribeFuture.isDone()) {
					result.trySuccess(false);
				    }
				}
			    }, time.get(), TimeUnit.MILLISECONDS);
		    futureRef.set(scheduledFuture);
		}
	    }
	});

	return result;
    }

    private void tryLockAsync(final AtomicLong time, final long leaseTime, final TimeUnit unit,
	    final Future<RedissonLockEntry> subscribeFuture, final Promise<Boolean> result, final String[] ownerKeys) {
	Future<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, ownerKeys);
	ttlFuture.addListener(new FutureListener<Long>() {
	    @Override
	    public void operationComplete(Future<Long> future) throws Exception {
		if (!future.isSuccess()) {
		    unsubscribe(subscribeFuture);
		    result.tryFailure(future.cause());
		    return;
		}

		Long ttl = future.getNow();
		// lock acquired
		if (ttl == null) {
		    unsubscribe(subscribeFuture);
		    result.trySuccess(true);
		    return;
		}

		if (time.get() < 0) {
		    unsubscribe(subscribeFuture);
		    result.trySuccess(false);
		    return;
		}

		// waiting for message
		final long current = System.currentTimeMillis();
		final RedissonLockEntry entry = getEntry();
		synchronized (entry) {
		    if (entry.getLatch().tryAcquire()) {
			tryLockAsync(time, leaseTime, unit, subscribeFuture, result, ownerKeys);
		    } else {
			final AtomicBoolean executed = new AtomicBoolean();
			final AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<ScheduledFuture<?>>();
			final Runnable listener = new Runnable() {
			    @Override
			    public void run() {
				executed.set(true);
				if (futureRef.get() != null) {
				    futureRef.get().cancel(false);
				}
				long elapsed = System.currentTimeMillis() - current;
				time.addAndGet(-elapsed);

				tryLockAsync(time, leaseTime, unit, subscribeFuture, result, ownerKeys);
			    }
			};
			entry.addListener(listener);

			long t = time.get();
			if (ttl >= 0 && ttl < time.get()) {
			    t = ttl;
			}
			if (!executed.get()) {
			    ScheduledFuture<?> scheduledFuture = commandExecutor.getConnectionManager().getGroup()
				    .schedule(new Runnable() {
					@Override
					public void run() {
					    synchronized (entry) {
						if (entry.removeListener(listener)) {
						    long elapsed = System.currentTimeMillis() - current;
						    time.addAndGet(-elapsed);

						    tryLockAsync(time, leaseTime, unit, subscribeFuture, result,
							    ownerKeys);
						}
					    }
					}
				    }, t, TimeUnit.MILLISECONDS);
			    futureRef.set(scheduledFuture);
			}
		    }
		}
	    }
	});
    }

}

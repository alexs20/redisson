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
 * {@link org.redisson.core.RLock} and {@link java.util.concurrent.locks.Lock} hence previous authors included.
 */

package org.redisson.core;

import java.util.concurrent.TimeUnit;

import io.netty.util.concurrent.Future;

/**
 * Distributed implementation of thread detached Lock similar to
 * {@link java.util.concurrent.locks.Lock} but performs thread locking using
 * {link RPadLockKey} as lock's ownership identifier.
 * 
 * @see org.redisson.RedissonPadLock
 * 
 * @author Doug Lea, Nikita Koksharov, Nickolay Borbit, Alexander Shulgin
 */

public interface RPadLock extends RExpirable {

    /**
     * Acquires the lock.
     *
     * <p>
     * If the lock is not available then the current thread becomes disabled for
     * thread scheduling purposes and lies dormant until the lock has been
     * acquired.
     *
     * If the lock is acquired, it is held until <code>unlock</code> is invoked,
     * or until leaseTime have passed since the lock was granted - whichever
     * comes first.
     *
     * @param ownerKeyss
     *            List of keys for this lock.
     * @param leaseTime
     *            the maximum time to hold the lock after granting it, before
     *            automatically releasing it if it hasn't already been released
     *            by invoking <code>unlock</code>. If leaseTime is -1, hold the
     *            lock until explicitly unlocked.
     * @param unit
     *            the time unit of the {@code leaseTime} argument
     * @throws InterruptedException
     *             - if the thread is interrupted before or during this method.
     */
    void lockInterruptibly(String[] ownerKeys, long leaseTime, TimeUnit unit) throws InterruptedException;

    /**
     * Returns <code>true</code> as soon as the lock is acquired. If the lock is
     * currently held by another owner in this or any other process in the
     * distributed system this method keeps trying to acquire the lock for up to
     * <code>waitTime</code> before giving up and returning <code>false</code>.
     * If the lock is acquired, it is held until <code>unlock</code> is invoked,
     * or until <code>leaseTime</code> have passed since the lock was granted -
     * whichever comes first.
     * 
     * @param ownerKeys
     *            List of keys for this lock.
     * @param waitTime
     *            the maximum time to acquire the lock
     * @param leaseTime
     * @param unit
     * @return
     * @throws InterruptedException
     */
    boolean tryLock(String[] ownerKeys, long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException;

    /**
     * Acquires the lock.
     *
     * <p>
     * If the lock is not available then the current thread becomes disabled for
     * thread scheduling purposes and lies dormant until the lock has been
     * acquired.
     *
     * If the lock is acquired, it is held until <code>unlock</code> is invoked,
     * or until leaseTime milliseconds have passed since the lock was granted -
     * whichever comes first.
     * 
     * @param ownerKeys
     *            List of keys for this lock.
     * @param leaseTime
     *            the maximum time to hold the lock after granting it, before
     *            automatically releasing it if it hasn't already been released
     *            by invoking <code>unlock</code>. If leaseTime is -1, hold the
     *            lock until explicitly unlocked.
     * @param unit
     *            the time unit of the {@code leaseTime} argument
     *
     */
    void lock(String[] ownerKeys, long leaseTime, TimeUnit unit);

    /**
     * Unlocks lock independently of state
     *
     */
    void forceUnlock();

    /**
     * Checks if this lock locked by any thread
     *
     * @return <code>true</code> if locked otherwise <code>false</code>
     */
    boolean isLocked();

    /**
     * Checks if this lock is held by the key
     * 
     * @param ownerKey
     *            a key for this lock.
     * @return @return <code>true</code> if held otherwise <code>false</code>
     */
    boolean isOwner(String ownerKey);

    /**
     * Number of holds on this lock by the current key
     * 
     * @param ownerKey
     *            a key for this lock.
     * @return holds or <code>0</code> if this lock is not held by current key
     */
    int getHoldCount(String ownerKey);

    /**
     * Asynchronous version of {@link #unlock(String [] )}
     */
    Future<Void> unlockAsync(String[] ownerKeys);

    /**
     * Asynchronous version of {@link #tryLock(String [] )}
     */
    Future<Boolean> tryLockAsync(String[] ownerKeys);

    /**
     * Asynchronous version of {@link #lock(String [] )}
     */
    Future<Void> lockAsync(String[] ownerKeys);

    /**
     * Asynchronous version of {@link #lock(String [] , long, TimeUnit)}
     */
    Future<Void> lockAsync(String[] ownerKeys, long leaseTime, TimeUnit unit);

    /**
     * Asynchronous version of {@link #tryLock(String [] , long, TimeUnit)}
     */
    Future<Boolean> tryLockAsync(String[] ownerKeys, long waitTime, TimeUnit unit);

    /**
     * Asynchronous version of
     * {@link #tryLock(String [] , long, long, TimeUnit)}
     */
    Future<Boolean> tryLockAsync(String[] ownerKeys, long waitTime, long leaseTime, TimeUnit unit);

    /**
     * Acquires the lock.
     *
     * <p>
     * If the lock is not available then the current thread becomes disabled for
     * thread scheduling purposes and lies dormant until the lock has been
     * acquired.
     * 
     * @param ownerKeys
     *            List of keys for this lock.
     */
    void lock(String[] ownerKeys);

    /**
     * Acquires the lock unless the current thread is
     * {@linkplain Thread#interrupt interrupted}.
     *
     * <p>
     * Acquires the lock if it is available and returns immediately.
     *
     * For more details see
     * {@see java.util.concurrent.locks.Lock#lockInterruptibly() } considering
     * that owner is a current thread.
     * 
     * @param ownerKeys
     *            List of keys for this lock.
     * @throws InterruptedException
     *             if the current thread is interrupted while acquiring the lock
     *             (and interruption of lock acquisition is supported)
     */
    void lockInterruptibly(String[] ownerKeys) throws InterruptedException;

    /**
     * Acquires the lock only if it is free at the time of invocation.
     *
     * <p>
     * Acquires the lock if it is available and returns immediately with the
     * value {@code true}. If the lock is not available then this method will
     * return immediately with the value {@code false}.
     * 
     * For more details see {@see java.util.concurrent.locks.Lock#tryLock() }
     * considering that owner is a current thread.
     * 
     * @param ownerKeys
     *            List of keys for this lock.
     * @return {@code true} if the lock was acquired and {@code false} otherwise
     */
    boolean tryLock(String[] ownerKeys);

    /**
     * Acquires the lock if it is free within the given waiting time and the
     * current thread has not been {@linkplain Thread#interrupt interrupted}.
     * 
     * For more details see {@see java.util.concurrent.locks.Lock#tryLock(long,
     * TimeUnit) } considering that owner is a current thread.
     * 
     * @param ownerKeys
     *            List of keys for this lock.
     * @param time
     *            the maximum time to wait for the lock
     * @param unit
     *            the time unit of the {@code time} argument
     * @return {@code true} if the lock was acquired and {@code false} if the
     *         waiting time elapsed before the lock was acquired
     *
     * @throws InterruptedException
     *             if the current thread is interrupted while acquiring the lock
     *             (and interruption of lock acquisition is supported)
     */
    boolean tryLock(String[] ownerKeys, long time, TimeUnit unit) throws InterruptedException;

    /**
     * Releases the lock.
     * 
     * @param ownerKeys
     *            List of keys for this lock.
     */
    void unlock(String[] ownerKeys);

}

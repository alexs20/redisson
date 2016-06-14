package org.redisson;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.junit.Assert;
import org.junit.Test;
import org.redisson.core.RLock;
import org.redisson.core.RPadLock;

public class RedissonPadLockTest extends BaseConcurrentTest {

    private String [] thisThreadKeys(){
	return new String[]{String.valueOf(Thread.currentThread().getId())};
    }
    
    @Test
    public void testTryLockWait() throws InterruptedException {
        testSingleInstanceConcurrency(1, r -> {
            RPadLock lock = r.getPadLock("padlock");
            lock.lock(thisThreadKeys());
        });

        RPadLock lock = redisson.getPadLock("padlock");
        
        long startTime = System.currentTimeMillis();
        lock.tryLock(thisThreadKeys(), 3, TimeUnit.SECONDS);
        assertThat(System.currentTimeMillis() - startTime).isBetween(2990L, 3100L);
    }
    
    @Test
    public void testDelete() {
	RPadLock lock = redisson.getPadLock("padlock");
        Assert.assertFalse(lock.delete());

        lock.lock(thisThreadKeys());
        Assert.assertTrue(lock.delete());
    }

    @Test
    public void testForceUnlock() {
        RPadLock lock = redisson.getPadLock("padlock");
        lock.lock(thisThreadKeys());
        lock.forceUnlock();
        Assert.assertFalse(lock.isLocked());

        lock = redisson.getPadLock("padlock");
        Assert.assertFalse(lock.isLocked());
    }

    @Test
    public void testExpire() throws InterruptedException {
        RPadLock lock = redisson.getPadLock("padlock");
        lock.lock(thisThreadKeys(), 2, TimeUnit.SECONDS);

        final long startTime = System.currentTimeMillis();
        Thread t = new Thread() {
            public void run() {
                RLock lock1 = redisson.getLock("lock");
                lock1.lock();
                long spendTime = System.currentTimeMillis() - startTime;
                Assert.assertTrue(spendTime < 2020);
                lock1.unlock();
            };
        };

        t.start();
        t.join();

        lock.unlock(thisThreadKeys());
    }

    @Test
    public void testAutoExpire() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        testSingleInstanceConcurrency(1, r -> {
            RLock lock = r.getLock("lock");
            lock.lock();
            latch.countDown();
        });

        Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
        RPadLock lock = redisson.getPadLock("padlock");
        Thread.sleep(TimeUnit.SECONDS.toMillis(RedissonLock.LOCK_EXPIRATION_INTERVAL_SECONDS + 1));
        Assert.assertFalse("Transient lock has not expired automatically", lock.isLocked());
    }

    @Test
    public void testGetHoldCount() {
        RPadLock lock = redisson.getPadLock("padlock");
        Assert.assertEquals(0, lock.getHoldCount(thisThreadKeys()[0]));
        lock.lock(thisThreadKeys());
        Assert.assertEquals(1, lock.getHoldCount(thisThreadKeys()[0]));
        lock.unlock(thisThreadKeys());
        Assert.assertEquals(0, lock.getHoldCount(thisThreadKeys()[0]));

        lock.lock(thisThreadKeys());
        lock.lock(thisThreadKeys());
        Assert.assertEquals(2, lock.getHoldCount(thisThreadKeys()[0]));
        lock.unlock(thisThreadKeys());
        Assert.assertEquals(1, lock.getHoldCount(thisThreadKeys()[0]));
        lock.unlock(thisThreadKeys());
        Assert.assertEquals(0, lock.getHoldCount(thisThreadKeys()[0]));
    }

    @Test
    public void testIsHeldByCurrentThreadOtherThread() throws InterruptedException {
        RPadLock lock = redisson.getPadLock("padlock");
        lock.lock(thisThreadKeys());

        Thread t = new Thread() {
            public void run() {
                RPadLock lock = redisson.getPadLock("padlock");
                Assert.assertFalse(lock.isOwner(thisThreadKeys()[0]));
            };
        };

        t.start();
        t.join();
        lock.unlock(thisThreadKeys());

        Thread t2 = new Thread() {
            public void run() {
                RPadLock lock = redisson.getPadLock("padlock");
                Assert.assertFalse(lock.isOwner(thisThreadKeys()[0]));
            };
        };

        t2.start();
        t2.join();
    }

    @Test
    public void testIsHeldByCurrentThread() {
        RPadLock lock = redisson.getPadLock("padlock");
        Assert.assertFalse(lock.isOwner(thisThreadKeys()[0]));
        lock.lock(thisThreadKeys());
        Assert.assertTrue(lock.isOwner(thisThreadKeys()[0]));
        lock.unlock(thisThreadKeys());
        Assert.assertFalse(lock.isOwner(thisThreadKeys()[0]));
    }

    @Test
    public void testIsLockedOtherThread() throws InterruptedException {
        RPadLock lock = redisson.getPadLock("padlock");
        lock.lock(thisThreadKeys());

        Thread t = new Thread() {
            public void run() {
                RPadLock lock = redisson.getPadLock("padlock");
                Assert.assertTrue(lock.isLocked());
            };
        };

        t.start();
        t.join();
        lock.unlock(thisThreadKeys());

        Thread t2 = new Thread() {
            public void run() {
                RPadLock lock = redisson.getPadLock("padlock");
                Assert.assertFalse(lock.isLocked());
            };
        };

        t2.start();
        t2.join();
    }

    @Test
    public void testIsLocked() {
        RPadLock lock = redisson.getPadLock("padlock");
        Assert.assertFalse(lock.isLocked());
        lock.lock(thisThreadKeys());
        Assert.assertTrue(lock.isLocked());
        lock.unlock(thisThreadKeys());
        Assert.assertFalse(lock.isLocked());
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnlockFail() throws InterruptedException {
        RPadLock lock = redisson.getPadLock("padlock");
        Thread t = new Thread() {
            public void run() {
                RPadLock lock = redisson.getPadLock("padlock");
                lock.lock(thisThreadKeys());

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                lock.unlock(thisThreadKeys());
            };
        };

        t.start();
        t.join(400);

        try {
            lock.unlock(thisThreadKeys());
        } catch (IllegalMonitorStateException e) {
            t.join();
            throw e;
        }
    }


    @Test
    public void testLockUnlock() {
	RPadLock lock = redisson.getPadLock("padlock1");
        lock.lock(thisThreadKeys());
        lock.unlock(thisThreadKeys());

        lock.lock(thisThreadKeys());
        lock.unlock(thisThreadKeys());
    }

    @Test
    public void testReentrancy() throws InterruptedException {
	RPadLock lock = redisson.getPadLock("padlock1");
        Assert.assertTrue(lock.tryLock(thisThreadKeys()));
        Assert.assertTrue(lock.tryLock(thisThreadKeys()));
        lock.unlock(thisThreadKeys());
        // next row  for test renew expiration tisk.
        //Thread.currentThread().sleep(TimeUnit.SECONDS.toMillis(RedissonLock.LOCK_EXPIRATION_INTERVAL_SECONDS*2));
        Thread thread1 = new Thread() {
            @Override
            public void run() {
        	RPadLock lock1 = redisson.getPadLock("padlock1");
                Assert.assertFalse(lock1.tryLock(thisThreadKeys()));
            }
        };
        thread1.start();
        thread1.join();
        lock.unlock(thisThreadKeys());
    }


    @Test
    public void testConcurrency_SingleInstance() throws InterruptedException {
        final AtomicInteger lockedCounter = new AtomicInteger();

        int iterations = 15;
        testSingleInstanceConcurrency(iterations, r -> {
            Lock lock = r.getLock("testConcurrency_SingleInstance");
            lock.lock();
            lockedCounter.incrementAndGet();
            lock.unlock();
        });

        Assert.assertEquals(iterations, lockedCounter.get());
    }

    @Test
    public void testConcurrencyLoop_MultiInstance() throws InterruptedException {
        final int iterations = 100;
        final AtomicInteger lockedCounter = new AtomicInteger();

        testMultiInstanceConcurrency(16, r -> {
            for (int i = 0; i < iterations; i++) {
                r.getLock("testConcurrency_MultiInstance1").lock();
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                lockedCounter.incrementAndGet();
                r.getLock("testConcurrency_MultiInstance1").unlock();
            }
        });

        Assert.assertEquals(16 * iterations, lockedCounter.get());
    }

    @Test
    public void testConcurrency_MultiInstance() throws InterruptedException {
        int iterations = 100;
        final AtomicInteger lockedCounter = new AtomicInteger();

        testMultiInstanceConcurrency(iterations, r -> {
            Lock lock = r.getLock("testConcurrency_MultiInstance2");
            lock.lock();
            lockedCounter.incrementAndGet();
            lock.unlock();
        });

        Assert.assertEquals(iterations, lockedCounter.get());
    }

    @Test
    public void testGetOwners() throws InterruptedException {
	RPadLock lock = redisson.getPadLock("padlock");
	Assert.assertEquals(0, lock.getOwners().length);

	String[] keys;

	lock.lock(new String[] { "123" });
	keys = lock.getOwners();
	Assert.assertEquals(1, keys.length);
	Assert.assertEquals("123", keys[0]);
	Assert.assertEquals(1, lock.getHoldCount("123"));

	lock.lock(new String[] { "123", "abc" });
	keys = lock.getOwners();
	Assert.assertEquals(2, keys.length);
	Arrays.sort(keys);
	Assert.assertArrayEquals(new String[] { "123", "abc" }, keys);
	Assert.assertEquals(2, lock.getHoldCount("123"));
	Assert.assertEquals(1, lock.getHoldCount("abc"));

	lock.unlock(new String[] { "123" });
	keys = lock.getOwners();
	Assert.assertEquals(2, keys.length);
	Arrays.sort(keys);
	Assert.assertArrayEquals(new String[] { "123", "abc" }, keys);
	Assert.assertEquals(1, lock.getHoldCount("123"));
	Assert.assertEquals(1, lock.getHoldCount("abc"));

	lock.unlock(new String[] { "123" });
	keys = lock.getOwners();
	Assert.assertEquals(1, keys.length);
	Assert.assertEquals("abc", keys[0]);
	Assert.assertEquals(0, lock.getHoldCount("123"));
	Assert.assertEquals(1, lock.getHoldCount("abc"));

	lock.unlock(new String[] { "abc" });
	keys = lock.getOwners();
	Assert.assertEquals(0, keys.length);
	Assert.assertEquals(0, lock.getHoldCount("123"));
	Assert.assertEquals(0, lock.getHoldCount("abc"));

    }

}

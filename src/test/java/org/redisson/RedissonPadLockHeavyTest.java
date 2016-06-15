package org.redisson;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.redisson.core.RBucket;
import org.redisson.core.RLock;
import org.redisson.core.RPadLock;
import org.redisson.core.RSemaphore;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class RedissonPadLockHeavyTest extends BaseTest {
    @Parameters
    public static Collection<Object[]> data() {

        return Arrays.asList(new Object[][] { { 2, 5000 }, { 5, 2000 }, { 10, 1000 }, { 20, 500 }, });
    }

    private ExecutorService executor;
    private int threads;
    private int loops;

    public RedissonPadLockHeavyTest(int threads, int loops) {
        this.threads = threads;
        executor = Executors.newFixedThreadPool(threads);
        this.loops = loops;
    }

    @Test
    public void lockUnlockRLock() throws Exception {
        for (int i = 0; i < threads; i++) {

            Runnable worker = new Runnable() {

                @Override
                public void run() {
                    for (int j = 0; j < loops; j++) {
                        RPadLock lock = redisson.getPadLock("RPLOCK_" + j);
                        lock.lock("123");
                        lock.lock("123", "abc");
                        lock.unlock("123");
                        lock.unlock("123", "abc");
                        lock.lock("123", "abc");
                        lock.unlock("123");
                        lock.unlock("abc");
                    }
                }
            };
            executor.execute(worker);
        }
        executor.shutdown();
        executor.awaitTermination(threads * loops, TimeUnit.SECONDS);

    }

}
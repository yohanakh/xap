package com.j_spaces.kernel.pool;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * Created by Barak Bar Orion
 * on 9/5/17.
 */
public class ElasticBlockingResourcePoolTest {
    private ResourcePool<DummyResource> pool;

    @Before
    public void setUp() throws Exception {
        final AtomicInteger ids = new AtomicInteger(0);
        pool = new ElasticBlockingResourcePool<DummyResource>(new IResourceFactory<DummyResource>() {
            @Override
            public DummyResource allocate() {
                return new DummyResource(ids.getAndIncrement());
            }
        }, 1, 5, 10,  new DummyResource[]{});
    }

    @Test(timeout = 2 * 1000L)
    public void allocateBlowHardLimit() throws Exception{
        DummyResource r1 = pool.getResource();
        DummyResource r2 = pool.getResource();
        DummyResource r3 = pool.getResource();
        DummyResource r4 = pool.getResource();
        DummyResource r5 = pool.getResource();
        DummyResource r6 = pool.getResource();
        DummyResource r7 = pool.getResource();
        DummyResource r8 = pool.getResource();
        DummyResource r9 = pool.getResource();
        assertThat(r1.isFromPool(), is(true));
        assertThat(r2.isFromPool(), is(true));
        assertThat(r3.isFromPool(), is(true));
        assertThat(r4.isFromPool(), is(true));
        assertThat(r5.isFromPool(), is(true));
        assertThat(r6.isFromPool(), is(false));
        assertThat(r7.isFromPool(), is(false));
        assertThat(r8.isFromPool(), is(false));
        assertThat(r9.isFromPool(), is(false));
        pool.freeResource(r1);
        assertThat(r1.isReleased(), is(true));
        r1 = pool.getResource();
        assertThat(r1.isFromPool(), is(true));
        pool.freeResource(r9);
        assertThat(r9.isReleased(), is(true));
        r9 = pool.getResource();
        assertThat(r9.isFromPool(), is(false));
    }

    @Test(timeout = 2 * 1000L)
    public void allocateAboveHardLimit() throws Exception{
        final List<DummyResource> resources = new ArrayList<DummyResource>();
        for(int i = 0; i < 10; ++i) {
            resources.add(pool.getResource());
        }
        final CyclicBarrier barrier = new CyclicBarrier(2);
        new Thread(){
            @Override
            public void run() {
                try {
                    barrier.await();
                    pool.freeResource(resources.get(0));
                    barrier.await();
                    pool.freeResource(resources.get(9));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        barrier.await();
        pool.getResource();
        barrier.await();
        pool.getResource();

    }


}
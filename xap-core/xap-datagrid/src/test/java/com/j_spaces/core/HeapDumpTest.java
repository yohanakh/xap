package com.j_spaces.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by Barak Bar Orion
 * on 8/28/16.
 *
 * @since 12.0
 */
@SuppressWarnings("unused")
public class HeapDumpTest {
    private HeapDump heapDump;

    @Before
    public void setUp() throws Exception {
        heapDump = new HeapDump();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testDisabled() throws Exception {
        heapDump.setEnabled(false);
        assertThat(heapDump.createDumpIfAppropriate(1), is(false));
    }
    @Test
    public void testOutOfCurrentHeaps() throws Exception {
        heapDump.setEnabled(true);
        heapDump.setCurrentHeaps(1);
        heapDump.setMaxHeaps(1);
        assertThat(heapDump.createDumpIfAppropriate(1), is(false));
    }

    @Test
    public void testQuietPeriodMillis() throws Exception {
        heapDump.setEnabled(true);
        heapDump.setCurrentHeaps(0);
        heapDump.setMaxHeaps(1);
        heapDump.lastDumpMillis = 1;
        assertThat(heapDump.createDumpIfAppropriate(1), is(false));
    }

    @Test
    public void testCreateDumpIfAppropriate() throws Exception {
        heapDump.setEnabled(true);
        heapDump.setCurrentHeaps(0);
        heapDump.setMaxHeaps(2);
        heapDump.setQuietPeriod(1);
        heapDump.lastDumpMillis = 1;
        assertThat(heapDump.createDumpIfAppropriate(2), is(true));
        assertThat(heapDump.getCurrentHeaps() , is(1));
        assertThat(heapDump.lastDumpMillis , is(2L));

        assertThat(heapDump.createDumpIfAppropriate(2), is(false));

        assertThat(heapDump.createDumpIfAppropriate(3), is(true));
        assertThat(heapDump.getCurrentHeaps() , is(2));
        assertThat(heapDump.lastDumpMillis , is(3L));
    }

}
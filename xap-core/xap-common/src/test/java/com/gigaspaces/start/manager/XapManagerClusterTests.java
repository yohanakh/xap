package com.gigaspaces.start.manager;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class XapManagerClusterTests {

    @Before
    public void setup() {
        System.clearProperty(XapManagerClusterInfo.SERVERS_PROPERTY);
        for (int i=1 ; i < 10 ; i++)
            System.clearProperty(XapManagerClusterInfo.SERVER_PROPERTY + "." + i);
    }

    @Test
    public void parseSingleHostShort() {
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "foo");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo("foo");
        Assert.assertEquals(1, cluster.getServers().length);
        Assert.assertEquals("foo", cluster.getServers()[0].getHost());
        Assert.assertSame(cluster.getCurrServer(), cluster.getServers()[0]);
    }

    @Test
    public void parseSingleHostShortNotCurr() {
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "foo");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo(null);
        Assert.assertEquals(1, cluster.getServers().length);
        Assert.assertEquals("foo", cluster.getServers()[0].getHost());
        Assert.assertNull(cluster.getCurrServer());
    }

    @Test
    public void parseMultipleHostsShort() {
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "a,b,c");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo("a");
        Assert.assertEquals(3, cluster.getServers().length);
        Assert.assertEquals("a", cluster.getServers()[0].getHost());
        Assert.assertEquals("b", cluster.getServers()[1].getHost());
        Assert.assertEquals("c", cluster.getServers()[2].getHost());
        Assert.assertSame(cluster.getCurrServer(), cluster.getServers()[0]);
    }

    @Test
    public void parseSingleHostFull() {
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".1", "foo");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo("foo");
        Assert.assertEquals(1, cluster.getServers().length);
        Assert.assertEquals("foo", cluster.getServers()[0].getHost());
        Assert.assertSame(cluster.getCurrServer(), cluster.getServers()[0]);
    }

    @Test
    public void parseMultipleHostsFull() {
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".1", "a");
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".2", "b");
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".3", "c");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo("b");
        Assert.assertEquals(3, cluster.getServers().length);
        Assert.assertEquals("a", cluster.getServers()[0].getHost());
        Assert.assertEquals("b", cluster.getServers()[1].getHost());
        Assert.assertEquals("c", cluster.getServers()[2].getHost());
        Assert.assertSame(cluster.getCurrServer(), cluster.getServers()[1]);
    }

    @Test
    public void parseMultipleHostsFullWithProperties() {
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".1", "a;lus=foo");
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".2", "b;rest=8080");
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".3", "c;rest=8081;zookeeper=1:2");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo("b");
        Assert.assertEquals(3, cluster.getServers().length);
        Assert.assertEquals("a", cluster.getServers()[0].getHost());
        Assert.assertEquals("foo", cluster.getServers()[0].getLookupService());
        Assert.assertEquals("b", cluster.getServers()[1].getHost());
        Assert.assertEquals("8080", cluster.getServers()[1].getAdminRest());
        Assert.assertEquals("c", cluster.getServers()[2].getHost());
        Assert.assertEquals("8081", cluster.getServers()[2].getAdminRest());
        Assert.assertEquals("1:2", cluster.getServers()[2].getZookeeper());
        Assert.assertSame(cluster.getCurrServer(), cluster.getServers()[1]);
    }

    @Test
    public void parseMultipleHostsFullWithGap() {
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".1", "a");
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".3", "c");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo("a");
        Assert.assertEquals(1, cluster.getServers().length);
        Assert.assertEquals("a", cluster.getServers()[0].getHost());
        Assert.assertSame(cluster.getCurrServer(), cluster.getServers()[0]);
    }

    @Test
    public void clusterFromShort() {
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "a");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo("a");
        Assert.assertEquals(1, cluster.getServers().length);
        Assert.assertEquals("a", cluster.getServers()[0].getHost());
        Assert.assertSame(cluster.getCurrServer(), cluster.getServers()[0]);
    }

    @Test
    public void clusterFromFull() {
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".1", "a");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo("a");
        Assert.assertEquals(1, cluster.getServers().length);
        Assert.assertEquals("a", cluster.getServers()[0].getHost());
        Assert.assertSame(cluster.getCurrServer(), cluster.getServers()[0]);
    }

    @Test
    public void ambiguousCluster() {
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "a");
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".1", "a");
        try {
            new XapManagerClusterInfo("foo");
            Assert.fail("Should have failed - ambiguous");
        } catch (Exception e) {
            System.out.println("Intercepted expected exception: " + e.getMessage());
        }
    }

    @Test
    public void validInvalidSizes() {
        XapManagerClusterInfo cluster;

        // Test 1:
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "a");
        cluster = new XapManagerClusterInfo("a");
        Assert.assertEquals(1, cluster.getServers().length);
        Assert.assertEquals("a", cluster.getServers()[0].getHost());
        Assert.assertSame(cluster.getCurrServer(), cluster.getServers()[0]);

        // Test 2:
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "a,b");
        try {
            cluster = new XapManagerClusterInfo("a");
            Assert.fail("Should have failed - unsupported cluster size");
        } catch (UnsupportedOperationException e) {
            System.out.println("Intercepted expected exception: " + e.getMessage());
        }

        // Test 3:
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "a,b,c");
        cluster = new XapManagerClusterInfo("b");
        Assert.assertEquals(3, cluster.getServers().length);
        Assert.assertEquals("a", cluster.getServers()[0].getHost());
        Assert.assertEquals("b", cluster.getServers()[1].getHost());
        Assert.assertEquals("c", cluster.getServers()[2].getHost());
        Assert.assertSame(cluster.getCurrServer(), cluster.getServers()[1]);

        // Test 4:
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "a,b,c,d");
        try {
            cluster = new XapManagerClusterInfo("a");
            Assert.fail("Should have failed - unsupported cluster size");
        } catch (UnsupportedOperationException e) {
            System.out.println("Intercepted expected exception: " + e.getMessage());
        }
    }
}

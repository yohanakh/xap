package com.gigaspaces.start.manager;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class XapManagerClusterTests {

    @Before
    public void setup() {
        System.clearProperty(XapManagerCluster.SERVERS_PROPERTY);
        for (int i=1 ; i < 10 ; i++)
            System.clearProperty(XapManagerCluster.SERVER_PROPERTY + "." + i);
    }

    @Test
    public void parseSingleHostShort() {
        System.setProperty(XapManagerCluster.SERVERS_PROPERTY, "foo");
        final List<XapManagerConfig> servers = XapManagerCluster.parseShort();
        Assert.assertEquals(1, servers.size());
        Assert.assertEquals("foo", servers.get(0).getHost());
        Assert.assertEquals(0, servers.get(0).getProperties().size());
    }

    @Test
    public void parseMultipleHostsShort() {
        System.setProperty(XapManagerCluster.SERVERS_PROPERTY, "a,b,c");
        final List<XapManagerConfig> servers = XapManagerCluster.parseShort();
        Assert.assertEquals(3, servers.size());
        Assert.assertEquals("a", servers.get(0).getHost());
        Assert.assertEquals("b", servers.get(1).getHost());
        Assert.assertEquals("c", servers.get(2).getHost());
        for (XapManagerConfig server : servers) {
            Assert.assertEquals(0, server.getProperties().size());
        }
    }

    @Test
    public void parseSingleHostFull() {
        System.setProperty(XapManagerCluster.SERVER_PROPERTY + ".1", "foo");
        final List<XapManagerConfig> servers = XapManagerCluster.parseFull();
        Assert.assertEquals(1, servers.size());
        Assert.assertEquals("foo", servers.get(0).getHost());
        Assert.assertEquals(0, servers.get(0).getProperties().size());
    }

    @Test
    public void parseMultipleHostsFull() {
        System.setProperty(XapManagerCluster.SERVER_PROPERTY + ".1", "a");
        System.setProperty(XapManagerCluster.SERVER_PROPERTY + ".2", "b");
        System.setProperty(XapManagerCluster.SERVER_PROPERTY + ".3", "c");
        final List<XapManagerConfig> servers = XapManagerCluster.parseFull();
        Assert.assertEquals(3, servers.size());
        Assert.assertEquals("a", servers.get(0).getHost());
        Assert.assertEquals("b", servers.get(1).getHost());
        Assert.assertEquals("c", servers.get(2).getHost());
        for (XapManagerConfig server : servers) {
            Assert.assertEquals(0, server.getProperties().size());
        }
    }

    @Test
    public void parseMultipleHostsFullWithProperties() {
        System.setProperty(XapManagerCluster.SERVER_PROPERTY + ".1", "a;foo=bar");
        System.setProperty(XapManagerCluster.SERVER_PROPERTY + ".2", "b;rest=8080");
        System.setProperty(XapManagerCluster.SERVER_PROPERTY + ".3", "c;rest=8081;zookeeper=1:2");
        final List<XapManagerConfig> servers = XapManagerCluster.parseFull();
        Assert.assertEquals(3, servers.size());
        Assert.assertEquals("a", servers.get(0).getHost());
        Assert.assertEquals(1, servers.get(0).getProperties().size());
        Assert.assertEquals("bar", servers.get(0).getProperties().getProperty("foo"));
        Assert.assertEquals("b", servers.get(1).getHost());
        Assert.assertEquals(1, servers.get(1).getProperties().size());
        Assert.assertEquals("8080", servers.get(1).getProperties().getProperty("rest"));
        Assert.assertEquals("c", servers.get(2).getHost());
        Assert.assertEquals(2, servers.get(2).getProperties().size());
        Assert.assertEquals("8081", servers.get(2).getProperties().getProperty("rest"));
        Assert.assertEquals("1:2", servers.get(2).getProperties().getProperty("zookeeper"));
    }

    @Test
    public void parseMultipleHostsFullWithGap() {
        System.setProperty(XapManagerCluster.SERVER_PROPERTY + ".1", "a");
        System.setProperty(XapManagerCluster.SERVER_PROPERTY + ".3", "c");
        final List<XapManagerConfig> servers = XapManagerCluster.parseFull();
        Assert.assertEquals(1, servers.size());
        Assert.assertEquals("a", servers.get(0).getHost());
        for (XapManagerConfig server : servers) {
            Assert.assertEquals(0, server.getProperties().size());
        }
    }

    @Test
    public void clusterFromShort() {
        System.setProperty(XapManagerCluster.SERVERS_PROPERTY, "a");
        XapManagerCluster cluster = new XapManagerCluster();
        Assert.assertEquals(1, cluster.getServers().length);
        Assert.assertEquals("a", cluster.getServers()[0].getHost());
    }

    @Test
    public void clusterFromFull() {
        System.setProperty(XapManagerCluster.SERVER_PROPERTY + ".1", "a");
        XapManagerCluster cluster = new XapManagerCluster();
        Assert.assertEquals(1, cluster.getServers().length);
        Assert.assertEquals("a", cluster.getServers()[0].getHost());
    }

    @Test
    public void ambiguousCluster() {
        System.setProperty(XapManagerCluster.SERVERS_PROPERTY, "a");
        System.setProperty(XapManagerCluster.SERVER_PROPERTY + ".1", "a");
        try {
            new XapManagerCluster();
            Assert.fail("Should have failed - ambiguous");
        } catch (Exception e) {
            System.out.println("Intercepted expected exception: " + e.getMessage());
        }
    }

    @Test
    public void validInvalidSizes() {
        XapManagerCluster cluster;

        // Test 1:
        System.setProperty(XapManagerCluster.SERVERS_PROPERTY, "a");
        cluster = new XapManagerCluster();
        Assert.assertEquals(1, cluster.getServers().length);
        Assert.assertEquals("a", cluster.getServers()[0].getHost());

        // Test 2:
        System.setProperty(XapManagerCluster.SERVERS_PROPERTY, "a,b");
        try {
            cluster = new XapManagerCluster();
            Assert.fail("Should have failed - unsupported cluster size");
        } catch (UnsupportedOperationException e) {
            System.out.println("Intercepted expected exception: " + e.getMessage());
        }

        // Test 3:
        System.setProperty(XapManagerCluster.SERVERS_PROPERTY, "a,b,c");
        cluster = new XapManagerCluster();
        Assert.assertEquals(3, cluster.getServers().length);
        Assert.assertEquals("a", cluster.getServers()[0].getHost());
        Assert.assertEquals("b", cluster.getServers()[1].getHost());
        Assert.assertEquals("c", cluster.getServers()[2].getHost());

        // Test 4:
        System.setProperty(XapManagerCluster.SERVERS_PROPERTY, "a,b,c,d");
        try {
            cluster = new XapManagerCluster();
            Assert.fail("Should have failed - unsupported cluster size");
        } catch (UnsupportedOperationException e) {
            System.out.println("Intercepted expected exception: " + e.getMessage());
        }
    }
}

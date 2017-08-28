package com.gigaspaces.start;

import org.jini.rio.boot.BootUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Niv Ingberg
 * @since 12.1
 */
public class XapNetworkInfo {
    private final String localHostName;
    private final String localHostCanonicalName;
    private final String hostId;
    private final InetAddress host;

    public XapNetworkInfo() {
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            this.localHostName = localHost.getHostName();
            this.localHostCanonicalName = localHost.getCanonicalHostName();
            this.hostId = BootUtil.getHostAddress();
            this.host = InetAddress.getByName(hostId);
        } catch (UnknownHostException e) {
            throw new IllegalStateException("Failed to get network information", e);
        }
    }

    public String getLocalHostName() {
        return localHostName;
    }

    public String getLocalHostCanonicalName() {
        return localHostCanonicalName;
    }

    public String getHostId() {
        return hostId;
    }

    public InetAddress getHost() {
        return host;
    }
}

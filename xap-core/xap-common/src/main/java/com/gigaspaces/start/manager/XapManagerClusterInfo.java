package com.gigaspaces.start.manager;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.start.SystemBoot;
import com.gigaspaces.start.SystemInfo;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class XapManagerClusterInfo {
    public static final String SERVERS_PROPERTY = "com.gs.manager.servers";
    public static final String SERVER_PROPERTY = "com.gs.manager.server";
    public static final String SERVERS_ENV_VAR = "XAP_MANAGER_SERVERS";
    public static final String SERVER_ENV_VAR = "XAP_MANAGER_SERVER";

    private static final Logger logger = Logger.getLogger(Constants.LOGGER_MANAGER);

    private final XapManagerConfig[] servers;

    public static XapManagerClusterInfo initialize(String currHost) {
        XapManagerClusterInfo result = new XapManagerClusterInfo();
        if (result.servers.length == 0 && SystemBoot.isManager()) {
            if (logger.isLoggable(Level.INFO))
                logger.log(Level.INFO, "Starting manager without configuration - defaulting to standalone manager on " + currHost);
            result = new XapManagerClusterInfo(new XapManagerConfig(currHost));
        }
        return result;
    }

    public XapManagerClusterInfo() {
        final List<XapManagerConfig> shortList = parseShort();
        final List<XapManagerConfig> fullList = parseFull();
        if (shortList.size() != 0 && fullList.size() != 0)
            throw new IllegalStateException("Ambiguous XAP manager cluster configuration (short and full)");
        final List<XapManagerConfig> servers = shortList.size() != 0 ? shortList : fullList;
        if (servers.size() != 0 && servers.size() != 1 && servers.size() != 3)
            throw new UnsupportedOperationException("Unsupported xap manager cluster size: " + servers.size());
        this.servers = servers.toArray(new XapManagerConfig[servers.size()]);
    }

    private XapManagerClusterInfo(XapManagerConfig server) {
        this.servers = new XapManagerConfig[] {server};
    }

    public static List<XapManagerConfig> parseShort() {
        final List<XapManagerConfig> result = new ArrayList<XapManagerConfig>();
        final String var = get(SERVERS_PROPERTY, SERVERS_ENV_VAR);
        if (var != null && var.length() != 0) {
            final String[] tokens = var.split(",");
            for (String token : tokens) {
                result.add(XapManagerConfig.parse(token));
            }
        }
        return result;
    }
    public static List<XapManagerConfig> parseFull() {
        final List<XapManagerConfig> result = new ArrayList<XapManagerConfig>();
        for (int i=1 ; i < 10 ; i++) {
            final String var = get(SERVER_PROPERTY + "." + i, SERVER_ENV_VAR + "_" + i);
            if (var != null && var.length() != 0)
                result.add(XapManagerConfig.parse(var));
            else
                break;
        }
        return result;
    }

    private static String get(String sysProp, String envVar) {
        return System.getProperty(sysProp, System.getenv(envVar));
    }

    public XapManagerConfig[] getServers() {
        return servers;
    }

    public XapManagerConfig findManagerByCurrHost(){
        return findManagerByHost(SystemInfo.singleton().network().getHost());
    }

    public XapManagerConfig findManagerByHost(InetAddress host) {
        for (XapManagerConfig server : servers) {
            if (server.getHost().equals(host.getHostName()) || server.getHost().equals(host.getHostAddress()))
                return server;
        }
        return null;
    }
}

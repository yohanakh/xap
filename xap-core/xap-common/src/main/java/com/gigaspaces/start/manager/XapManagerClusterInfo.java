package com.gigaspaces.start.manager;

import java.util.ArrayList;
import java.util.List;

public class XapManagerClusterInfo {
    public static final String SERVERS_PROPERTY = "com.gs.manager.servers";
    public static final String SERVER_PROPERTY = "com.gs.manager.server";
    public static final String SERVERS_ENV_VAR = "XAP_MANAGER_SERVERS";
    public static final String SERVER_ENV_VAR = "XAP_MANAGER_SERVER";

    private final XapManagerConfig[] servers;

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
}

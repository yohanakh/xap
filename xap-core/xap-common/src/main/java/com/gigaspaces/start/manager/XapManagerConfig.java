package com.gigaspaces.start.manager;

import java.util.Properties;

public class XapManagerConfig {
    // List of servers.
    // Each server has a host, and a map of component-to-port
    private final String host;
    private final Properties properties;

    private XapManagerConfig(String host, Properties properties) {
        this.host = host;
        this.properties = properties;
    }

    public static XapManagerConfig parse(String s) {
        final String[] tokens = s.split(";");
        final String host = tokens[0];
        final Properties properties = new Properties();
        for (int i=1 ; i < tokens.length ; i++) {
            String token = tokens[i];
            int pos = token.indexOf('=');
            String key = token.substring(0, pos);
            String value = token.substring(pos+1);
            properties.setProperty(key, value);
        }
        return new XapManagerConfig(host, properties);
    }

    public String getHost() {
        return host;
    }

    public Properties getProperties() {
        return properties;
    }
}

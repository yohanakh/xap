/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.start.manager;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.start.SystemBoot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

public class XapManagerClusterInfo {
    public static final String SERVERS_PROPERTY = "com.gs.manager.servers";
    public static final String SERVER_PROPERTY = "com.gs.manager.server";
    public static final String SERVERS_ENV_VAR = "XAP_MANAGER_SERVERS";
    public static final String SERVER_ENV_VAR = "XAP_MANAGER_SERVER";

    private static final Logger logger = Logger.getLogger(Constants.LOGGER_MANAGER);

    private final XapManagerConfig currServer;
    private final XapManagerConfig[] servers;

    public static XapManagerClusterInfo initialize(String currHost) {
        return new XapManagerClusterInfo(currHost);
    }

    public XapManagerClusterInfo(String currHost) {
        final Collection<XapManagerConfig> servers = parse();
        if (servers.isEmpty() && SystemBoot.isManager()) {
            if (logger.isLoggable(Level.INFO))
                logger.log(Level.INFO, "Starting manager without configuration - defaulting to standalone manager on " + currHost);
            servers.add(new XapManagerConfig(currHost));
        }
        if (servers.size() != 0 && servers.size() != 1 && servers.size() != 3)
            throw new UnsupportedOperationException("Unsupported xap manager cluster size: " + servers.size());
        this.servers = servers.toArray(new XapManagerConfig[servers.size()]);
        this.currServer = findManagerByHost(currHost);
    }

    public XapManagerConfig[] getServers() {
        return servers;
    }

    public XapManagerConfig getCurrServer() {
        return currServer;
    }

    private static Collection<XapManagerConfig> parse() {
        final Collection<XapManagerConfig> shortList = parseShort();
        final Collection<XapManagerConfig> fullList = parseFull();
        if (shortList.size() != 0 && fullList.size() != 0)
            throw new IllegalStateException("Ambiguous XAP manager cluster configuration (short and full)");
        return shortList.size() != 0 ? shortList : fullList;
    }

    private static Collection<XapManagerConfig> parseShort() {
        final Collection<XapManagerConfig> result = new ArrayList<XapManagerConfig>();
        final String var = get(SERVERS_PROPERTY, SERVERS_ENV_VAR);
        if (var != null && var.length() != 0) {
            final String[] tokens = var.split(",");
            for (String token : tokens) {
                result.add(XapManagerConfig.parse(token));
            }
        }
        return result;
    }

    private static Collection<XapManagerConfig> parseFull() {
        final Collection<XapManagerConfig> result = new ArrayList<XapManagerConfig>();
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

    private XapManagerConfig findManagerByHost(String host) {
        for (XapManagerConfig server : servers) {
            if (server.getHost().equals(host))
                return server;
        }
        return null;
    }
}

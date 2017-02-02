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


package org.openspaces.pu.container.support;

import com.j_spaces.core.client.SpaceURL;
import org.openspaces.core.cluster.ClusterInfo;

/**
 * {@link org.openspaces.core.cluster.ClusterInfo} parser that parses -cluster parameter and
 * transforms it into a cluster info.
 *
 * <p> The following arguments to the -cluster parameters are allowed:
 * <code>total_members=1,1</code> (1,1 is an example value), <code>id=1</code> (1 is an example
 * value), <code>backup_id=1</code> (1 is an example value) and <code>schema=primary_backup</code>
 * (primary_backup is an example value).
 *
 * <p>The container allows not to specify explicit <code>instanceId</code> or <code>backupId</code>.
 * In this case, it will create several processing units that run embedded within the JVM.
 *
 * @author kimchy
 */
public abstract class ClusterInfoParser {

    public static final String CLUSTER_PARAMETER_TOTALMEMBERS = "total_members";
    public static final String CLUSTER_PARAMETER_INSTANCEID = "id";
    public static final String CLUSTER_PARAMETER_BACKUPID = "backup_id";
    public static final String CLUSTER_PARAMETER_CLUSTERSCHEMA = "schema";

    public static ClusterInfo parse(CommandLineParser.Parameter[] params) throws IllegalArgumentException {
        ClusterInfo clusterInfo = null;
        for (CommandLineParser.Parameter param : params) {
            if (!param.getName().equalsIgnoreCase("cluster")) {
                continue;
            }

            if (clusterInfo == null) {
                clusterInfo = new ClusterInfo();
            }

            if (param.getArguments().length == 0) {
                throw new IllegalArgumentException("cluster parameter should have at least one parameter");
            }

            for (int j = 0; j < param.getArguments().length; j++) {
                String clusterParameter = param.getArguments()[j];
                int equalsIndex = clusterParameter.indexOf("=");
                if (equalsIndex == -1) {
                    throw new IllegalArgumentException("Cluster parameter [" + clusterParameter
                            + "] is malformed, must have a name=value syntax");
                }
                String clusterParamName = clusterParameter.substring(0, equalsIndex);
                String clusterParamValue = clusterParameter.substring(equalsIndex + 1);
                if (CLUSTER_PARAMETER_TOTALMEMBERS.equalsIgnoreCase(clusterParamName)) {
                    int commaIndex = clusterParamValue.indexOf(',');
                    if (commaIndex == -1) {
                        clusterInfo.setNumberOfInstances(Integer.valueOf(clusterParamValue));
                    } else {
                        String numberOfInstances = clusterParamValue.substring(0, commaIndex);
                        String numberOfBackups = clusterParamValue.substring(commaIndex + 1);
                        clusterInfo.setNumberOfInstances(Integer.valueOf(numberOfInstances));
                        clusterInfo.setNumberOfBackups(Integer.valueOf(numberOfBackups));
                    }
                } else if (CLUSTER_PARAMETER_INSTANCEID.equalsIgnoreCase(clusterParamName)) {
                    clusterInfo.setInstanceId(Integer.valueOf(clusterParamValue));
                } else if (CLUSTER_PARAMETER_BACKUPID.equalsIgnoreCase(clusterParamName)) {
                    clusterInfo.setBackupId(Integer.valueOf(clusterParamValue));
                } else if (CLUSTER_PARAMETER_CLUSTERSCHEMA.equalsIgnoreCase(clusterParamName)) {
                    clusterInfo.setSchema(clusterParamValue);
                } else {
                    throw new IllegalArgumentException("deploy parameter property name [" + clusterParamName
                            + "] is invalid");
                }
            }
        }
        if (clusterInfo != null) {
            if (clusterInfo.getNumberOfInstances() == null) {
                throw new IllegalArgumentException("unspecified cluster total number of instances");
            }
        }
        return clusterInfo;
    }

    public static ClusterInfo parse(SpaceURL spaceURL) {
        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.setSchema(spaceURL.getProperty(SpaceURL.CLUSTER_SCHEMA));
        clusterInfo.setInstanceId(parseInt(spaceURL.getProperty(SpaceURL.CLUSTER_MEMBER_ID), 1));
        clusterInfo.setBackupId(parseInt(spaceURL.getProperty(SpaceURL.CLUSTER_BACKUP_ID), 0));

        String s = spaceURL.getProperty(SpaceURL.CLUSTER_TOTAL_MEMBERS);
        String[] tokens = (s != null && s.length() != 0 ? s : "1,0").split(",");
        clusterInfo.setNumberOfInstances(Integer.parseInt(tokens[0]));
        clusterInfo.setNumberOfBackups(tokens.length > 1 ? Integer.parseInt(tokens[1]) : 0);

        return clusterInfo;
    }

    private static int parseInt(String s, int defaultValue) {
        return s == null || s.length() == 0 ? defaultValue : Integer.parseInt(s);
    }

    /**
     * Guess the cluster schema if not set. If the number of instances is higher than 1 and the
     * number of backups it higher than 0, the cluster schema will be <code>partitioned</code>.
     */
    public static void guessSchema(ClusterInfo clusterInfo) {
        if (clusterInfo.getSchema() != null) {
            return;
        }
        if (clusterInfo.getNumberOfInstances() != null && clusterInfo.getNumberOfInstances() > 1) {
            clusterInfo.setSchema("partitioned");
        } else if (clusterInfo.getNumberOfInstances() != null && clusterInfo.getNumberOfInstances() == 1 &&
                clusterInfo.getNumberOfBackups() != null && clusterInfo.getNumberOfBackups() > 0) {
            clusterInfo.setSchema("partitioned");
        }
    }
}

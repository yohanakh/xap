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
package com.gigaspaces.metrics;

/**
 * Metric constants
 * @since 12.1
 */
public interface MetricConstants {
    String SPACE_METRIC_NAME = "space";
    String BLOBSTORE_METRIC_NAME = "blobstore";
    String MIRROR_METRIC_NAME = "mirror";
    String REPLICATION_METRIC_NAME = "replication";
    String REDO_LOG_METRIC_NAME = "redo-log";
    String OPERATIONS_METRIC_NAME = "operations";
    String CONNECTIONS_METRIC_NAME = "connections";
    String ACTIVE_CONNECTIONS_METRIC_NAME = "active-connections";
}
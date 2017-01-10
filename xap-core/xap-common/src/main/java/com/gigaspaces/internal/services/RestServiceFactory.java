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
package com.gigaspaces.internal.services;

import com.gigaspaces.start.ClasspathBuilder;

/**
 * @author Niv Ingberg
 * @since 12.1
 */
public class RestServiceFactory extends ServiceFactory {
    @Override
    protected String getServiceName() {
        return "REST";
    }

    @Override
    protected String getServiceClassName() {
        return "com.gigaspaces.rest.Starter";
    }

    @Override
    protected void initializeClasspath(ClasspathBuilder classpath) {
        classpath.appendPlatform("admin-rest")
                .appendPlatform("service-grid/xap-admin.jar")
                .appendPlatform("service-grid/xap-service-grid.jar")
                .appendPlatform("logger")
                .appendOptional("security")
                // Required jars: spring-context-*, spring-beans-*, spring-core-*, commons-logging-*, xap-datagrid, xap-asm, xap-trove
                .appendRequired(ClasspathBuilder.startsWithFilter("spring-", "commons-", "xap-datagrid", "xap-openspaces", "xap-asm", "xap-trove"))
                .appendOptional("jetty")
                .appendOptional("jackson");

    }
}

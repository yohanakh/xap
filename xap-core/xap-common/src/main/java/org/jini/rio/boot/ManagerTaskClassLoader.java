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

package org.jini.rio.boot;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by tamirs
 * on 11/8/16.
 */
@com.gigaspaces.api.InternalApi
public class ManagerTaskClassLoader{

    final private static Logger logger = Logger.getLogger("com.gigaspaces.lrmi.classloading.level");

    private final Map<String, ClassLoader> classVersionToClassLoaderMap;

    private final ClassLoader classLoader;

    public ManagerTaskClassLoader(ClassLoader serviceClassLoader){
        classVersionToClassLoaderMap = new HashMap<String, ClassLoader>();
        classLoader  = serviceClassLoader;
    }

    public ClassLoader getTaskClassLoader(SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer) {
        if(logger.isLoggable(Level.FINEST)){
            logger.finest("Search for class-loader with version ["+supportCodeChangeAnnotationContainer.getVersion()+"] ");
        }
        if(supportCodeChangeAnnotationContainer.getVersion().isEmpty()){ // one time
            TaskClassLoader taskClassLoader = new TaskClassLoader(new URL[]{}, classLoader);
            if(logger.isLoggable(Level.FINEST)){
                logger.finest("Created new class-loader for for one time use. New class loader is " + taskClassLoader );
            }
            return taskClassLoader;
        }
        else { // cache class loader
            String classVersion = supportCodeChangeAnnotationContainer.getVersion();
            ClassLoader cachedClassLoader = classVersionToClassLoaderMap.get(classVersion);
            if(cachedClassLoader != null){ // cached class loader
                if(logger.isLoggable(Level.FINEST)){
                    logger.finest("Found cached class-loader: " + cachedClassLoader + ", with version: " + classVersion);
                }
                return cachedClassLoader;
            }
            else { // version not cached
                TaskClassLoader taskClassLoader = new TaskClassLoader(new URL[]{}, classLoader);
                classVersionToClassLoaderMap.put(supportCodeChangeAnnotationContainer.getVersion(), taskClassLoader);
                if(logger.isLoggable(Level.INFO)){
                    logger.info("Did not found class-loader with version " + supportCodeChangeAnnotationContainer.getVersion() + ". Created new class-loader: " + taskClassLoader + ", and cache it");
                }
                return taskClassLoader;
            }
        }
    }
}

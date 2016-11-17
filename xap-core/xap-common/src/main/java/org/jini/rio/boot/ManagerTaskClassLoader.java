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
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by tamirs
 * on 11/8/16.
 */
@com.gigaspaces.api.InternalApi
public class ManagerTaskClassLoader{

    final private static Logger logger = Logger.getLogger("com.gigaspaces.lrmi.classloading.level");

    @SuppressWarnings("FieldCanBeLocal")
    private final int DEFAULT_MAX_NUMBER_OF_CLASS_LOADERS = 3;

    private final ClassLoader defaultClassLoader;
    private final ConcurrentHashMap<String, TaskClassLoader> versionToTaskClassLoaderMap;
    private int maxClassLoaders;

    ManagerTaskClassLoader(ClassLoader serviceClassLoader){
        defaultClassLoader = serviceClassLoader;
        versionToTaskClassLoaderMap = new ConcurrentHashMap<String, TaskClassLoader>();
        maxClassLoaders = DEFAULT_MAX_NUMBER_OF_CLASS_LOADERS;
    }

    ClassLoader getTaskClassLoader(SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer) {
        if(logger.isLoggable(Level.FINEST)){
            logger.finest("Search for class-loader with version ["+supportCodeChangeAnnotationContainer.getVersion()+"] ");
        }
        if(supportCodeChangeAnnotationContainer.getVersion().isEmpty()){ // one time
            TaskClassLoader taskClassLoader = new TaskClassLoader(new URL[]{}, defaultClassLoader);
            if(logger.isLoggable(Level.FINEST)){
                logger.finest("Created new class-loader for for one time use. New class loader is " + taskClassLoader );
            }
            return taskClassLoader;
        }
        else { // might be already loaded
            String classVersion = supportCodeChangeAnnotationContainer.getVersion();
            ClassLoader cachedClassLoader = versionToTaskClassLoaderMap.get(classVersion);
            if(cachedClassLoader != null){ // cached class loader
                if(logger.isLoggable(Level.FINEST)){
                    logger.finest("Found class-loader: " + cachedClassLoader + ", with version: " + classVersion);
                }
                return cachedClassLoader;
            }
            synchronized (this){
                // if 2 threads try to insert to map, while map was full,
                // both threads had same version on task -
                // then first one got the lock update the map, and the second one only need to do get on map
                TaskClassLoader tcl = versionToTaskClassLoaderMap.get(classVersion);
                if(tcl != null){
                    return tcl;
                }
                return insertToMap(classVersion);
            }
        }
    }

    private TaskClassLoader insertToMap(String version) {
        if(versionToTaskClassLoaderMap.size() == this.maxClassLoaders){
            removeOldestTaskClassLoader();
        }
        TaskClassLoader taskClassLoader = new TaskClassLoader(new URL[]{}, defaultClassLoader);
        versionToTaskClassLoaderMap.put(version, taskClassLoader);
        if(logger.isLoggable(Level.INFO)){
            logger.info("Did not found class-loader with version ["+version+"], Created and Inserted new class-loader ["+taskClassLoader+"]");
        }
        if(logger.isLoggable(Level.FINEST)){
            logger.info("Class-loaders map: " + versionToTaskClassLoaderMap);
        }
        return taskClassLoader;
    }

    private void removeOldestTaskClassLoader() {
        String oldestVersion = findOldestVersion();
        TaskClassLoader removedTaskClassLoader = versionToTaskClassLoaderMap.remove(oldestVersion);
        if(logger.isLoggable(Level.INFO)){
            logger.info("Class-loaders map had reached her limit, removed oldest class-loader ["+removedTaskClassLoader+"] with version ["+oldestVersion+"]");
        }
    }

    private String findOldestVersion() {
        long min = System.currentTimeMillis();
        String oldestVersion = null;
        for (Map.Entry<String, TaskClassLoader> versionTaskClassLoaderEntry : versionToTaskClassLoaderMap.entrySet()) {
            String version = versionTaskClassLoaderEntry.getKey();
            TaskClassLoader taskClassLoader = versionTaskClassLoaderEntry.getValue();
            long taskClassLoaderLoadTime = taskClassLoader.getLoadTime();
            if(taskClassLoaderLoadTime < min){
                min = taskClassLoaderLoadTime;
                oldestVersion = version;
            }
        }
        return oldestVersion;
    }

    public SpaceInstanceRemoteClassLoaderInfo createSpaceInstanceRemoteClassLoaderInfo(){
        return new SpaceInstanceRemoteClassLoaderInfo(
                new RemoteClassLoaderInfo(defaultClassLoader.toString()),
                createClassLoaderInfoMap(versionToTaskClassLoaderMap),
                this.maxClassLoaders);
    }

    private HashMap<String, RemoteClassLoaderInfo> createClassLoaderInfoMap(ConcurrentHashMap<String, TaskClassLoader> versionToTaskClassLoaderMap) {
        HashMap<String, RemoteClassLoaderInfo> map = new HashMap<String, RemoteClassLoaderInfo>();
        for (Map.Entry<String, TaskClassLoader> versionTaskClassLoaderEntry : versionToTaskClassLoaderMap.entrySet()) {
            String version = versionTaskClassLoaderEntry.getKey();
            TaskClassLoader taskClassLoader = versionTaskClassLoaderEntry.getValue();
            map.put(version, taskClassLoader.createRemoteClassLoaderInfo());
        }
        return map;
    }

    public int getMaxClassLoaders() {
        return maxClassLoaders;
    }

    public void setMaxClassLoaders(int maxClassLoaders) {
        this.maxClassLoaders = maxClassLoaders;
        if(logger.isLoggable(Level.INFO)){
            logger.info("Set max class-loaders capacity to ["+maxClassLoaders+"]");
        }
    }
}

package org.jini.rio.boot;

import org.junit.Test;

import java.net.URL;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by tamirs
 * on 12/4/16.
 */
public class ManagerTaskClassLoaderTest {
    ClassLoader  defaultClassLoader = new TaskClassLoader(new URL[]{}, getClass().getClassLoader());
    boolean supportCodeChange = true;
    int maxClassLoaders = 2;

    ManagerTaskClassLoader managerTaskClassLoader =
            new ManagerTaskClassLoader(defaultClassLoader, supportCodeChange, maxClassLoaders);

    @Test
    public void getTaskClassLoader() throws Exception {

        // insert for the first time version 5
        SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer_5 = new SupportCodeChangeAnnotationContainer("5");
        ClassLoader taskClassLoader_5 = managerTaskClassLoader.getTaskClassLoader(supportCodeChangeAnnotationContainer_5);

        // insert for the first time version 6
        SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer_6 = new SupportCodeChangeAnnotationContainer("6");
        ClassLoader taskClassLoader_6 = managerTaskClassLoader.getTaskClassLoader(supportCodeChangeAnnotationContainer_6);

        // insert for the first time version 5
        SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer_7 = new SupportCodeChangeAnnotationContainer("7");
        ClassLoader taskClassLoader_7 = managerTaskClassLoader.getTaskClassLoader(supportCodeChangeAnnotationContainer_7);

        // assert default
        assertTrue(managerTaskClassLoader.getDefaultClassLoader().equals(defaultClassLoader));

        // assert saved class loaders
        ConcurrentHashMap<String, TaskClassLoader> versionToTaskClassLoaderMap = managerTaskClassLoader.getVersionToTaskClassLoaderMap();
        assertTrue(versionToTaskClassLoaderMap.size() == 2);

        // assert versions
        Set<String> keySet = versionToTaskClassLoaderMap.keySet();
        assertTrue(keySet.contains("6"));
        assertTrue(keySet.contains("7"));
        assertFalse(keySet.contains("5"));

        // assert saved class loaders
        Collection<TaskClassLoader> values = versionToTaskClassLoaderMap.values();
        assertTrue(values.size() == 2);
        //noinspection SuspiciousMethodCalls
        assertTrue(values.contains(taskClassLoader_6));
        //noinspection SuspiciousMethodCalls
        assertTrue(values.contains(taskClassLoader_7));
        //noinspection SuspiciousMethodCalls
        assertFalse(values.contains(taskClassLoader_5));

        // assert max class loaders
        assertTrue(managerTaskClassLoader.getMaxClassLoaders() == 2);

        // assert support code change
        assertTrue(managerTaskClassLoader.isSupportCodeChange());

        // get saved class loader with version 6
        ClassLoader taskClassLoader_6_secondTime = managerTaskClassLoader.getTaskClassLoader(supportCodeChangeAnnotationContainer_6);
        assertTrue(taskClassLoader_6_secondTime.equals(taskClassLoader_6));

        // get saved class loader with version 7
        ClassLoader taskClassLoader_7_secondTime = managerTaskClassLoader.getTaskClassLoader(supportCodeChangeAnnotationContainer_7);
        assertTrue(taskClassLoader_7_secondTime.equals(taskClassLoader_7));

        SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer_8 = new SupportCodeChangeAnnotationContainer("8");
        managerTaskClassLoader.getTaskClassLoader(supportCodeChangeAnnotationContainer_8);

        // assert insert of new version
        //noinspection SuspiciousMethodCalls
        assertFalse(managerTaskClassLoader.getVersionToTaskClassLoaderMap().contains(taskClassLoader_6));


    }

    @Test(expected = UnsupportedOperationException.class)
    public void getTaskClassLoaderDisabledSupportCodeChange() throws Exception {
        ClassLoader  defaultClassLoader = new TaskClassLoader(new URL[]{}, getClass().getClassLoader());
        boolean supportCodeChange = false;
        int maxClassLoaders = 2;

        //noinspection ConstantConditions
        ManagerTaskClassLoader managerTaskClassLoader = new ManagerTaskClassLoader(defaultClassLoader, supportCodeChange, maxClassLoaders);

        // insert for the first time version 5
        SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer_5 = new SupportCodeChangeAnnotationContainer("5");

        assertFalse(managerTaskClassLoader.isSupportCodeChange());
        managerTaskClassLoader.getTaskClassLoader(supportCodeChangeAnnotationContainer_5);

    }

}
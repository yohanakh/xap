/*******************************************************************************
 * Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
package com.gigaspaces.classloader;

import com.gigaspaces.internal.io.BootIOUtils;
import org.jini.rio.boot.LoggableClassLoader;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
public class CustomURLClassLoader extends URLClassLoader implements LoggableClassLoader {

    protected final Logger logger;
    private final String name;

    public CustomURLClassLoader(String name, URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.name = name;
        this.logger = Logger.getLogger("com.gigaspaces.classloader." + name);
        if (logger.isLoggable(Level.FINE)) {
            StringBuilder sb = new StringBuilder("Created [urls=" + urls.length + "]");
            final String prefix = BootIOUtils.NEW_LINE + "\t";
            for (URL url : urls) {
                sb.append(prefix).append(url);
            }

            logger.log(Level.FINE, sb.toString());
        }
    }

    @Override
    public String toString() {
        return (super.toString() + " [name=" + name + "]");
    }

    @Override
    public String getLogName() {
        return this.name;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (logger.isLoggable(Level.FINEST))
            this.logger.log(Level.FINEST, "loadClass(" + name + ")");
        return super.loadClass(name, resolve);
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        if (logger.isLoggable(Level.FINE))
            this.logger.log(Level.FINE, "findClass(" + name + ")");
        return super.findClass(name);
    }

    @Override
    public URL getResource(String name) {
        if (logger.isLoggable(Level.FINEST))
            this.logger.log(Level.FINEST, "getResource(" + name + ")");
        return super.getResource(name);
    }

    @Override
    public URL findResource(String name) {
        if (logger.isLoggable(Level.FINE))
            this.logger.log(Level.FINE, "findResource(" + name + ")");
        return super.findResource(name);
    }

    /**
     * Alternate method for getURLs (some implementations override it in a non-traceable fashion)
     * @return
     */
    public URL[] getSearchPath() {
        return getURLs();
    }
}

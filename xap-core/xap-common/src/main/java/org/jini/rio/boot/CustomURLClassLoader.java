/*******************************************************************************
 * Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
package org.jini.rio.boot;

import com.gigaspaces.internal.io.BootIOUtils;

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
        this.logger = Logger.getLogger("com.gigaspaces.CustomURLClassLoader." + name);
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
        if (logger.isLoggable(Level.FINE))
            this.logger.log(Level.FINE, "loadClass(" + name + ")");
        return super.loadClass(name, resolve);
    }
}

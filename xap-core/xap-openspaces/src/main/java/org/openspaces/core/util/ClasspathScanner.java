/*
 * Copyright (c) 2008-2017, GigaSpaces Technologies, Inc. All Rights Reserved.
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
package org.openspaces.core.util;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.filter.TypeFilter;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Set;

/**
 * Utility class for scanning classpath using Spring's ClassPathScanningCandidateComponentProvider
 *
 * @author Niv Ingberg
 * @since 12.2
 */
public class ClasspathScanner {
    private final ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);

    public ClasspathScanner include(TypeFilter typeFilter) {
        scanner.addIncludeFilter(typeFilter);
        return this;
    }

    public ClasspathScanner urls(URL... urls) {
        scanner.setResourceLoader(new PathMatchingResourcePatternResolver(new URLClassLoader(urls, null)));
        return this;
    }

    public Set<BeanDefinition> scan() {
        return scan("");
    }

    public Set<BeanDefinition> scan(String basePackage) {
        return scanner.findCandidateComponents(basePackage);
    }
}

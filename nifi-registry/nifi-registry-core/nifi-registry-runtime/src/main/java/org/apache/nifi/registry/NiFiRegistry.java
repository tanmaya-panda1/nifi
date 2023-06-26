/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.registry;

import org.apache.nifi.registry.jetty.JettyServer;
import org.apache.nifi.registry.jetty.handler.HandlerProvider;
import org.apache.nifi.registry.jetty.handler.StandardHandlerProvider;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.crypto.BootstrapFileCryptoKeyProvider;
import org.apache.nifi.registry.security.crypto.CryptoKeyProvider;
import org.apache.nifi.registry.security.crypto.MissingCryptoKeyException;
import org.apache.nifi.registry.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Main entry point for NiFiRegistry.
 */
public class NiFiRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(NiFiRegistry.class);

    public static final String BOOTSTRAP_PORT_PROPERTY = "nifi.registry.bootstrap.listen.port";


    private final JettyServer server;
    private final BootstrapListener bootstrapListener;
    private volatile boolean shutdown = false;

    public NiFiRegistry(final NiFiRegistryProperties properties, CryptoKeyProvider masterKeyProvider) throws IOException, IllegalArgumentException {

        Thread.setDefaultUncaughtExceptionHandler((t, e) -> LOGGER.error("An Unknown Error Occurred in Thread {}", t, e));

        // register the shutdown hook
        // shutdown the jetty server
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownHook));

        final String bootstrapPort = System.getProperty(BOOTSTRAP_PORT_PROPERTY);
        if (bootstrapPort != null) {
            try {
                final int port = Integer.parseInt(bootstrapPort);

                if (port < 1 || port > 65535) {
                    throw new RuntimeException("Failed to start NiFi Registry because system property '" + BOOTSTRAP_PORT_PROPERTY + "' is not a valid integer in the range 1 - 65535");
                }

                bootstrapListener = new BootstrapListener(this, port);
                bootstrapListener.start();
            } catch (final NumberFormatException nfe) {
                throw new RuntimeException("Failed to start NiFi Registry because system property '" + BOOTSTRAP_PORT_PROPERTY + "' is not a valid integer in the range 1 - 65535");
            }
        } else {
            LOGGER.info("NiFi Registry started without Bootstrap Port information provided; will not listen for requests from Bootstrap");
            bootstrapListener = null;
        }

        // delete the web working dir - if the application does not start successfully
        // the web app directories might be in an invalid state. when this happens
        // jetty will not attempt to re-extract the war into the directory. by removing
        // the working directory, we can be assured that it will attempt to extract the
        // war every time the application starts.
        File webWorkingDir = properties.getWebWorkingDirectory();
        FileUtils.deleteFilesInDirectory(webWorkingDir, null, LOGGER, true, true);
        FileUtils.deleteFile(webWorkingDir, LOGGER, 3);

        // redirect JUL log events
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        final long startTime = System.nanoTime();

        final String docsDirectory = System.getProperty(NiFiRegistryProperties.NIFI_REGISTRY_BOOTSTRAP_DOCS_DIR_PROPERTY,
                NiFiRegistryProperties.RELATIVE_DOCS_LOCATION);
        final HandlerProvider handlerProvider = new StandardHandlerProvider(masterKeyProvider, docsDirectory);
        server = new JettyServer(properties, handlerProvider);

        if (shutdown) {
            LOGGER.info("NiFi Registry has been shutdown via NiFi Registry Bootstrap. Will not start Controller");
        } else {
            server.start();

            if (bootstrapListener != null) {
                bootstrapListener.sendStartedStatus(true);
            }

            final long duration = System.nanoTime() - startTime;
            final double durationSeconds = TimeUnit.NANOSECONDS.toMillis(duration) / 1000.0;
            LOGGER.info("Started Application in {} seconds ({} ns)", durationSeconds, duration);
        }
    }

    protected void shutdownHook() {
        try {
            this.shutdown = true;

            LOGGER.info("Initiating shutdown of Jetty web server...");
            if (server != null) {
                server.stop();
            }
            if (bootstrapListener != null) {
                bootstrapListener.stop();
            }
            LOGGER.info("Jetty web server shutdown completed (nicely or otherwise).");
        } catch (final Throwable t) {
            LOGGER.warn("Problem occurred ensuring Jetty web server was properly terminated due to " + t);
        }
    }

    /**
     * Main entry point of the application.
     *
     * @param args things which are ignored
     */
    public static void main(String[] args) {
        LOGGER.info("Launching NiFi Registry...");

        final CryptoKeyProvider masterKeyProvider;
        final NiFiRegistryProperties properties;
        final ClassLoader sensitivePropProviderClassLoader;
        try {
            masterKeyProvider = getMasterKeyProvider();
            sensitivePropProviderClassLoader = createSensitivePropertiesProviderClassLoader();
            properties = initializeProperties(masterKeyProvider, sensitivePropProviderClassLoader);
        } catch (final IllegalArgumentException iae) {
            throw new RuntimeException("Unable to load properties: " + iae, iae);
        }

        try {
            new NiFiRegistry(properties, masterKeyProvider);
        } catch (final Throwable t) {
            LOGGER.error("Failure to launch NiFi Registry due to " + t, t);
        }
    }

    public static CryptoKeyProvider getMasterKeyProvider() {
        final String bootstrapConfigFilePath = System.getProperty(NiFiRegistryProperties.NIFI_REGISTRY_BOOTSTRAP_FILE_PATH_PROPERTY,
                NiFiRegistryProperties.RELATIVE_BOOTSTRAP_FILE_LOCATION);
        CryptoKeyProvider masterKeyProvider = new BootstrapFileCryptoKeyProvider(bootstrapConfigFilePath);
        LOGGER.info("Read property protection key from {}", bootstrapConfigFilePath);
        return masterKeyProvider;
    }

    public static NiFiRegistryProperties initializeProperties(CryptoKeyProvider masterKeyProvider, final ClassLoader sensitivePropertyProviderClassLoader) {
        String key = CryptoKeyProvider.EMPTY_KEY;
        try {
            key = masterKeyProvider.getKey();
        } catch (MissingCryptoKeyException e) {
            LOGGER.debug("CryptoKeyProvider provided to initializeProperties method was empty - did not contain a key.");
            // Do nothing. The key can be empty when it is passed to the loader as the loader will only use it if any properties are protected.
        }

        final String nifiRegistryPropertiesFilePath = System.getProperty(NiFiRegistryProperties.NIFI_REGISTRY_PROPERTIES_FILE_PATH_PROPERTY,
                NiFiRegistryProperties.RELATIVE_PROPERTIES_FILE_LOCATION);

        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(sensitivePropertyProviderClassLoader);

        try {
            final Class<?> propsLoaderClass = Class.forName("org.apache.nifi.registry.properties.NiFiRegistryPropertiesLoader", true, sensitivePropertyProviderClassLoader);
            final Method withKeyMethod = propsLoaderClass.getMethod("withKey", String.class);
            final Object loaderInstance = withKeyMethod.invoke(null, key);
            final Method loadMethod = propsLoaderClass.getMethod("load", String.class);
            final NiFiRegistryProperties properties = (NiFiRegistryProperties) loadMethod.invoke(loaderInstance, nifiRegistryPropertiesFilePath);
            LOGGER.info("Application Properties loaded [{}]", properties.size());
            return properties;
        } catch (InvocationTargetException wrappedException) {
            final String msg = "There was an issue decrypting protected properties";
            throw new IllegalArgumentException(msg, wrappedException.getCause() == null ? wrappedException : wrappedException.getCause());
        } catch (final IllegalAccessException | NoSuchMethodException | ClassNotFoundException reex) {
            final String msg = "Unable to access properties loader in the expected manner - apparent classpath or build issue";
            throw new IllegalArgumentException(msg, reex);
        } catch (final RuntimeException e) {
            final String msg = "There was an issue decrypting protected properties";
            throw new IllegalArgumentException(msg, e);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    public static ClassLoader createSensitivePropertiesProviderClassLoader() {
        final List<URL> urls = new ArrayList<>();
        try (final Stream<Path> files = Files.list(Paths.get("lib/spp"))) {
            files.forEach(p -> {
                try {
                    urls.add(p.toUri().toURL());
                } catch (final MalformedURLException mef) {
                    LOGGER.warn("Unable to load bootstrap library [{}]", p.getFileName(), mef);
                }
            });
        } catch (IOException ioe) {
            LOGGER.warn("Unable to access lib/spp to create sensitive property provider classloader", ioe);
        }
        return new URLClassLoader(urls.toArray(new URL[0]), Thread.currentThread().getContextClassLoader());
    }

}

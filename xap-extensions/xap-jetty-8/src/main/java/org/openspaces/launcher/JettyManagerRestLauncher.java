package org.openspaces.launcher;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.nio.filters.SelfSignedCertificate;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.manager.XapManagerConfig;
import com.j_spaces.kernel.SystemProperties;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Yohana Khoury
 * @since 12.1
 */
public class JettyManagerRestLauncher implements Closeable {
    private static final Logger logger = Logger.getLogger(Constants.LOGGER_MANAGER);

    private AbstractXmlApplicationContext application;
    private Server server;

    public static void main(String[] args) {
        final JettyManagerRestLauncher starter = new JettyManagerRestLauncher();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                starter.close();
            }
        });
    }

    /**
     * NOTE: This ctor is also called via reflection from SystemConfig
     */
    @SuppressWarnings("WeakerAccess")
    public JettyManagerRestLauncher() {
        try {
            final XapManagerConfig config = SystemInfo.singleton().getManagerClusterInfo().getCurrServer();
            if (config == null) {
                logger.severe("Cannot start server  - this host is not part of the xap managers configuration");
                System.exit(1);
            }
            String customJettyPath = System.getProperty(SystemProperties.MANAGER_REST_JETTY_CONFIG);
            if (customJettyPath != null) {
                logger.info("Loading jetty configuration from " + customJettyPath);
                this.application = new FileSystemXmlApplicationContext(customJettyPath);
                this.server = this.application.getBean(Server.class);
            } else {
                this.server = new Server();
            }
            if (!server.isStarted()) {
                if (server.getConnectors() == null || server.getConnectors().length == 0) {
                    initConnectors(server, config);
                }
                if (server.getHandler() == null) {
                    initWebApps(server);
                }
                server.start();
            }
            if (logger.isLoggable(Level.INFO)) {
                Collection<String> webAppsPaths = Collections.singletonList("v1");
                for (String webAppsPath : webAppsPaths) {
                    String connectors = "";
                    for (Connector connector : server.getConnectors()) {
                        String protocol = connector instanceof SslSelectChannelConnector ? "https" : "http";
                        String connectorDesc = protocol + "://" + connector.getHost() + ":" + connector.getLocalPort() + "/" + webAppsPath;
                        connectors = connectors.isEmpty() ? connectorDesc : connectors + ", " + connectorDesc;
                    }
                    logger.info("Started at " + connectors);
                }
            }
        }catch(Exception e){
            logger.log(Level.SEVERE, e.toString(), e);
            System.exit(-1);
        }
    }

    private void initConnectors(Server server, XapManagerConfig config)
            throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        final String host = config.getHost();
        final int port = Integer.parseInt(config.getAdminRest());
        final SslContextFactory sslContextFactory = createSslContextFactoryIfNeeded();

        Connector connector = sslContextFactory != null ? new SslSelectChannelConnector(sslContextFactory) : new SelectChannelConnector();
        connector.setPort(port);
        if (host != null)
            connector.setHost(host);
        connector.setMaxIdleTime(30000);
        server.addConnector(connector);
    }

    private void initWebApps(Server server) {
        ContextHandlerCollection handler = new ContextHandlerCollection();
        File webApps = new File(SystemInfo.singleton().locations().getLibPlatform() + "/admin-rest/webapps");
        FilenameFilter warFilesFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".war");
            }
        };
        for (File file : webApps.listFiles(warFilesFilter)) {
            WebAppContext webApp = new WebAppContext();
            webApp.setContextPath("/" + file.getName().replace(".war", ""));
            webApp.setWar(file.getAbsolutePath());
            webApp.setThrowUnavailableOnStartupException(true);
            handler.addHandler(webApp);
        }
        server.setHandler(handler);
    }

    private SslContextFactory createSslContextFactoryIfNeeded()
            throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        boolean sslEnabled = Boolean.getBoolean(SystemProperties.MANAGER_REST_SSL_ENABLED);
        if (!sslEnabled)
            return null;
        SslContextFactory sslContextFactory = new SslContextFactory();
        String keyStorePath = System.getProperty(SystemProperties.MANAGER_REST_SSL_KEYSTORE_PATH);
        String password = System.getProperty(SystemProperties.MANAGER_REST_SSL_KEYSTORE_PASSWORD);

        if (keyStorePath != null && new File(keyStorePath).exists()) {
            sslContextFactory.setKeyStorePath(keyStorePath);
            sslContextFactory.setKeyStorePassword(password);
        } else {
            sslContextFactory.setKeyStore(SelfSignedCertificate.keystore());
            sslContextFactory.setKeyStorePassword("foo");
            logger.info("SSL Keystore was not provided - Self-signed certificate was generated");
        }

        return sslContextFactory;
    }

    @Override
    public void close() {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                logger.warning("Failed to stop server: " + e);
            }
        }
        if (this.application != null)
            this.application.destroy();
    }
}

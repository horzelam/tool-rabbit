package com.horzelam.tool.factory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicies;
import net.jodah.lyra.config.RetryPolicies;
import net.jodah.lyra.util.Duration;

public class ChannelFactory {

    private static Logger logger = (Logger) LoggerFactory.getLogger(ChannelFactory.class);
    /**
     * host name property.
     */
    public static final String HOST_KEY = "rabbit.host";

    /**
     * user name property.
     */
    public static final String USER_KEY = "rabbit.user";

    /**
     * password property.
     */
    public static final String PASSWD_KEY = "rabbit.passwd";

    private static final long BACKOFF_SEC = 20;

    public static Channel createChannel(Properties properties) {
        try {
            Connection connection = ChannelFactory
                    .initConnection(properties.getProperty(ChannelFactory.HOST_KEY),
                            properties.getProperty(ChannelFactory.USER_KEY),
                            properties.getProperty(ChannelFactory.PASSWD_KEY));
            return connection.createChannel();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create Channel instance " + e.getMessage(), e);
        }
    }

    public static Channel createChannel(String queueConfigPath) throws IOException {
        Properties props = ChannelFactory.readProperties(queueConfigPath);
        return ChannelFactory.createChannel(props);
    }

    public static Connection initConnection(String rabbitmqHost, String user, String pass) {
        Config config = new Config().withRecoveryPolicy(
                RecoveryPolicies.recoverAlways().withBackoff(Duration.seconds(3), Duration.seconds(BACKOFF_SEC)))
                .withRetryPolicy(
                        RetryPolicies.retryAlways().withBackoff(Duration.seconds(3), Duration.seconds(BACKOFF_SEC)));

        ConnectionOptions options = new ConnectionOptions().withHost(rabbitmqHost).withUsername(user)
                .withPassword(pass);

        Connection newConnection = null;
        try {
            newConnection = Connections.create(options, config);
        } catch (Exception e) {
            logger.error("Unable to create connection", e);
        }
        return newConnection;
    }

    public static Properties readProperties(String fileName) throws IOException {
        Properties props = System.getProperties();
        logger.info("Reading from file: " + fileName);
        props.putAll(read(fileName));

        return props;
    }
    
    
    public static Properties read(String filename) throws IOException {
        Properties p = new Properties();

        FileInputStream fi = new FileInputStream(filename);
        try {
            p.load(fi);
            fi.close();
            return p;
        } catch (IOException e) {
            logger.warn("could not load file " + String.valueOf(filename) + " " + e.getMessage());
        } finally {
            fi.close();
        }

        // if this did not work try to load it with guava resources
        p.load(new StringReader(Resources.toString(Resources.getResource(filename), Charsets.UTF_8)));

        return p;
    }

}

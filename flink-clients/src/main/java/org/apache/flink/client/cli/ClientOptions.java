package org.apache.flink.client.cli;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.util.Optional;

/**
 * @author : tong.wang
 * @version : 1.0.0
 * @since : 5/19/20 10:55 PM
 */
public class ClientOptions {

    /**
     * Timeout for all blocking calls on the client side.
     */
    public static final ConfigOption<Duration> CLIENT_TIMEOUT = ConfigOptions
            .key("client.timeout")
            .durationType()
            .defaultValue(Duration.ofSeconds(60))
            .withDescription("Timeout for all blocking calls on the client side.");

    public static Duration getClientTimeout(Configuration configuration) {
        Optional<Duration> timeoutOptional = configuration.getOptional(CLIENT_TIMEOUT);
        if (timeoutOptional.isPresent()) {
            return timeoutOptional.get();
        } else {
           Optional<String> akkaClientTimeout = configuration.getOptional(AkkaOptions.AKKA_CLIENT_TIMEOUT);
           if (akkaClientTimeout.isPresent()) {
               return TimeUtils.parseDuration(akkaClientTimeout.get());
           } else {
               return CLIENT_TIMEOUT.defaultValue();
           }
        }
    }

}
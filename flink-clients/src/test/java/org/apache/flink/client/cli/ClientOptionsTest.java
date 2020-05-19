package org.apache.flink.client.cli;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.queryablestate.network.Client;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import java.time.Duration;

import static org.junit.Assert.assertEquals;

/**
 * @author : tong.wang
 * @version : 1.0.0
 * @since : 5/19/20 11:25 PM
 */
@RunWith(JUnit4.class)
public class ClientOptionsTest {

    @Test
    public void testGetClientTimeout() {
        Configuration configuration = new Configuration();
        configuration.set(ClientOptions.CLIENT_TIMEOUT, Duration.ofSeconds(10));

        assertEquals(ClientOptions.getClientTimeout(configuration), Duration.ofSeconds(10));

        configuration = new Configuration();
        configuration.set(AkkaOptions.AKKA_CLIENT_TIMEOUT, "20 s");
        assertEquals(ClientOptions.getClientTimeout(configuration), Duration.ofSeconds(20));

        configuration = new Configuration();
        assertEquals(ClientOptions.getClientTimeout(configuration), ClientOptions.CLIENT_TIMEOUT.defaultValue());
    }
}
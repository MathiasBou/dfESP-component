package com.sas.o2.cep;

import java.net.URISyntaxException;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

public class DfESPComponentIT extends CamelTestSupport {

    private static final String MOCK_OUT = "mock:out";
    private static final String MOCK_IN = "mock:result";
    /**
     * Folder with test files as uri.
     */
    private static String PCRF_TEST_FILES;

    @BeforeClass
    public static void beforeClass() throws URISyntaxException  {
        PCRF_TEST_FILES = ClassLoader.getSystemResource("pcrf-files").toURI().toString();
    }

    @Test
    public void testdfESP() throws Exception {
        MockEndpoint mockIn = getMockEndpoint(MOCK_IN);
        MockEndpoint mockOut = getMockEndpoint(MOCK_OUT);
        mockIn.expectedMessageCount(5);
        mockOut.expectedMessageCount(5);
        //data in mock are available after this call
        assertMockEndpointsSatisfied();
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from(PCRF_TEST_FILES + "?noop=true").unmarshal().gzip()
                  .to("dfESP://192.168.190.128:55555/TEST/TEST/PCRF_DATA_USAGE_STREAM?mode=insertAddId")
                  .to(MOCK_IN);
                from("dfESP://192.168.190.128:55555/TEST/TEST/PCRF_DATA_USAGE_STREAM")
                    .to(MOCK_OUT);
            }
        };
    }
}

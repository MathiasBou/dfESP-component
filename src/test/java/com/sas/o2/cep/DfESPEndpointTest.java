package com.sas.o2.cep;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
public class DfESPEndpointTest {

    @Test
    public void testGetProject(){
        final DfESPComponent component = new DfESPComponent();
        final DfESPEndpoint myDfESPEndpoint = new DfESPEndpoint("dfESP://172.16.253.128:55555/IMEI_TRACK_EVENTS/IMEI_TRACK_EVENTS_QUERY/IMEI_TRACK_STREAM", component);

        assertThat (myDfESPEndpoint.getProjectName(), is("dfESP://172.16.253.128:55555/IMEI_TRACK_EVENTS"));
    }
}

package com.sas.o2.cep;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.camel.Endpoint;
import org.apache.camel.impl.UriEndpointComponent;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Represents the component that manages {@link DfESPEndpoint}.
 */
public class DfESPComponent extends UriEndpointComponent {

    /**
     * To work with/extending {@link UriEndpointComponent}.
     */
    public DfESPComponent() {
        super(DfESPEndpoint.class);
        // eliminates logging to java.util.logger
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        // redirects all java.util.logger stuff to slf4j
        SLF4JBridgeHandler.install();
    }

    /**
     * All esp urls must match this pattern. It should reflect
     * dfESP://host:port/project/contquery/window.
     */
    public static final String URL_PATTERN = "^(dfESP)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]/[a-zA-Z0-9_]*/[a-zA-Z0-9_]*/[a-zA-Z0-9_]*";

    @Override
    protected final Endpoint createEndpoint(final String uri, final String remaining,
                                            final Map<String, Object> parameters) throws Exception {
        //Strip off parameters
        String uriWithoutParameters = uri.split("\\?")[0];
        // check uri against pattern
        if (!Pattern.matches(URL_PATTERN, uriWithoutParameters)) {
            throw new IllegalArgumentException("Given url ("
                                               + uriWithoutParameters
                                               + ") does not macth required pattern: dfESP://host:port/project/contquery/window");
        }
        Endpoint endpoint = new DfESPEndpoint(uriWithoutParameters, this);
        setProperties(endpoint, parameters);
        return endpoint;
    }
}

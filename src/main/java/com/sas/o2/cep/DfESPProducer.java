package com.sas.o2.cep;

import java.io.ByteArrayInputStream;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.UUID;
import java.util.logging.Level;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sas.esp.api.dfESPException;
import com.sas.esp.api.pubsub.clientCallbacks;
import com.sas.esp.api.pubsub.clientFailureCodes;
import com.sas.esp.api.pubsub.clientFailures;
import com.sas.esp.api.pubsub.clientGDStatus;
import com.sas.esp.api.pubsub.dfESPclient;
import com.sas.esp.api.pubsub.dfESPclientHandler;
import com.sas.esp.api.server.eventblock.EventBlockType;
import com.sas.esp.api.server.ReferenceIMPL.dfESPevent;
import com.sas.esp.api.server.ReferenceIMPL.dfESPeventblock;
import com.sas.esp.api.server.ReferenceIMPL.dfESPlibrary;
import com.sas.esp.api.server.ReferenceIMPL.dfESPschema;

/**
 * The dfESP producer. Takes exchanges representing csv files and sends them as
 * "blocks" to esp engine.
 */
public class DfESPProducer extends DefaultProducer implements clientCallbacks {
    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(DfESPProducer.class);
    /**
     * Default date time format.
     */
    public static final String DATA_FORMAT = "yyyy-MM-dd HH:mm:ss";
    /**
     * Url of target window (should be a "window-source" in model) in esp.
     */
    private String endpointUri;
    /**
     * Represents the target of data (window-source in esp model).
     */
    private dfESPschema schema;
    /**
     * Mode how to send data to esp.
     */
    private Mode mode;
    /**
     * Handles client and connection.
     */
    private dfESPclientHandler handler;
    /**
     * Used to publish data.
     */
    private dfESPclient client;
    private DfESPEndpoint endpoint;
    private String projectName;

    /**
     *
     * @param endpoint
     *            this is created by this endpoint - contains parameters
     */
    public DfESPProducer(final DfESPEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
        projectName = endpoint.getProjectName();
        endpointUri = endpoint.getEndpointUri();
        LOG.info("Creating producer for endpoint " + endpointUri);
        mode = endpoint.getMode();
        if (mode == null) {
            throw new NullPointerException(
                                           "Mode to insert data is not set. Use mode=insert|upsert|insertAddId|delete parameter in uri.");
        }
        LOG.debug("Mode for endpoint " + endpointUri + " set to " + mode.name());
    }

    @Override
    public final void process(final Exchange exchange) throws Exception {
        // TODO the flag is set by consumer. At the moment the only way to
        // detect engine down.
        if (DfESPEndpoint.isEngineDown()) {
            throw new EspConnectionLostException(
                                                 "Seems that cep engine is down. Check/ restart engine and then restart adapter.");
        }
        byte[] csv = exchange.getIn().getBody(byte[].class);

        Scanner scanner = new Scanner(new ByteArrayInputStream(csv));
        ArrayList<dfESPevent> eventList = new ArrayList<>();
        while (scanner.hasNextLine()) {
            String lineForCep = scanner.nextLine();
            //lineForCep = lineForCep.replace("\\", "\\\\");		//escape backslash \
            //lineForCep = lineForCep.replace("\"", "\\\"");		//escape quotes " uncommented Mathias Bouten

            // ignore empty lines
            if (!lineForCep.trim().isEmpty()) {
                switch (mode) {
                case insertAddId:
                    lineForCep = UUID.randomUUID().toString().concat("," + lineForCep);
                default:
                    // fall thru
                    lineForCep = mode.prefix() + lineForCep;
                    break;
                }
                eventList.add(new dfESPevent(schema, lineForCep, ','));
                endpoint.countEventSent();
                LOG.trace("Added line for cep: " + lineForCep);

            }
        }
        // don't send empty blocks
        if (!eventList.isEmpty()) {
            boolean quiesce = false;
            if (endpoint.isQuiesce()) {
                // in case of split property != null and we wait for split to
                // complete. If not split property should be null and we quiesce
                // on every exchange.
                if (exchange.getProperty("CamelSplitComplete", Boolean.class) == null)
                    quiesce = true;
                else {
                    quiesce = exchange.getProperty("CamelSplitComplete", Boolean.class);
                }

            }
            publishBlock(new dfESPeventblock(eventList, EventBlockType.ebt_NORMAL), quiesce);
            endpoint.countBlockSent();
        }
        // important!
        scanner.close();
    }

    @Override
    protected final void doStart() throws Exception {
        super.doStart();
        LOG.debug("Publisher connecting to " + endpointUri);
        handler = new dfESPclientHandler();
        /*
         * all jul logging will be redirected to central slf4j logger. too many
         * messages will impact performance. see MainApp and
         * http://www.slf4j.org/legacy.html#jul-to-slf4j
         */
        // ignoring return - has no sense
        handler.init(Level.WARNING);

        dfESPlibrary.setDateFormat(new SimpleDateFormat(DATA_FORMAT));

        ArrayList<String> schemaVector;
        try {
            schemaVector = handler.queryMeta(endpointUri + "?get=schema");
            // we noticed that schema vector could be null
            if (schemaVector == null) {
                throw new NullPointerException("dfESP handler returned a null schema vector for unknown reason. Cep seems to be offline or not reachable.");
            }
        } catch (UnknownHostException e) {
            throw new IllegalStateException("Problem on fetching meta data from " + endpointUri + ", cause: ", e);
        }
        try {
            schema = new dfESPschema(schemaVector.get(0));
        } catch (dfESPException e) {
            throw new IllegalStateException("Problem on creating schema, cause: ", e);
        }
        client = handler.publisherStart(endpointUri, this, null);
        if (client == null) {
            throw new IllegalStateException("Client object creation for connection to CEP engine not possible");
        }
        try {
            handler.connect(client);
        } catch (SocketException | UnknownHostException e) {
            throw new IllegalStateException("Can't connect to esp engine.");
        }
    }

    /**
     * Publishes a block of events to this' client's window.
     *
     * @param block
     *            to be published.
     */
    private void publishBlock(final dfESPeventblock block, boolean quiesce) {
        LOG.debug("Publishing block with id " + block.getTID() + " to window " + endpointUri);
        if (!handler.publisherInject(client, block)) {
            LOG.error("Error on sending eventblock to cep engine. Window: " + endpointUri);
        }
        if (quiesce) {
            try {
                handler.quiesceProject(projectName, client);
            } catch (UnknownHostException e) {
                throw new IllegalStateException("Problem on quiesceProject from " + endpointUri + ", cause: ", e);
            }
        }
    }

    @Override
    protected final void doStop() throws Exception {
        super.doStop();
        LOG.debug("Disconnecting publisher from " + endpointUri);
        // TODO probably only one of this 2 calls needed - always logs error
        handler.disconnect(client, true);
        // handler.stop(client, true);

    }

    /**
     * @param clientStatus
     *            enum with ACK, NACK or READY
     * @param blockId
     *            id of block
     * @param arg2
     *            "the user context pointer passed to C_dfESPGDpublisherStart()"
     *            - should be null here
     */
    @Override
    public final void dfESPGDpublisherCB_func(final clientGDStatus clientStatus, final long blockId, final Object arg2) {
        LOG.debug("Received " + clientStatus.name() + " for event block id: " + blockId);
    }

    @Override
    public final void dfESPpubsubErrorCB_func(final clientFailures failure, final clientFailureCodes failureCode,
                                              final Object arg2) {
        LOG.error("Publisher/producer received error from cep: Error " + failure.name() + " with code "
                  + failureCode.name());
    }

    @Override
    public final void dfESPsubscriberCB_func(final dfESPeventblock arg0, final dfESPschema arg1, final Object arg2) {
        LOG.error("should not be called - i am a publisher not an subscriber.");
    }

}

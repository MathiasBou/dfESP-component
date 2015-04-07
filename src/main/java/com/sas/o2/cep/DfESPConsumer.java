package com.sas.o2.cep;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sas.esp.api.dfESPException;
import com.sas.esp.api.pubsub.clientCallbacks;
import com.sas.esp.api.pubsub.clientFailureCodes;
import com.sas.esp.api.pubsub.clientFailures;
import com.sas.esp.api.pubsub.clientGDStatus;
import com.sas.esp.api.pubsub.dfESPclient;
import com.sas.esp.api.pubsub.dfESPclientHandler;
import com.sas.esp.api.server.datavar.FieldTypes;
import com.sas.esp.api.server.ReferenceIMPL.dfESPevent;
import com.sas.esp.api.server.ReferenceIMPL.dfESPeventblock;
import com.sas.esp.api.server.ReferenceIMPL.dfESPschema;

/**
 * The dfESP consumer. For each event consumed and processed an {@link List
 * &lt;EspDataItem&gt;} is sent as exchange.
 */
public class DfESPConsumer extends DefaultConsumer implements clientCallbacks {
    /**
     * Cep will buffer at max this number of events on output. If this size is
     * reached cep will throttle input automatically.
     */
    private static final int MAX_QUEUE_SIZE_CEP = 128;
    /**
     * Data from esp is sent with opcode as prefix like "I,N:". This specifies
     * the character index after this prefix.
     */
    private static final Integer INDEX_TO_CUT = 4;
    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(DfESPConsumer.class);
    /**
     * Uri of endpoint - the esp window that delivers the data.
     */
    private String endpointUri;
    /**
     * Created this, contains all uri parameters.
     */
    private DfESPEndpoint endpoint;
    private dfESPclientHandler handler;
    private dfESPclient client;
    /**
     * Names for all fields. Extracted from schema - window's meta data.
     */
    private ArrayList<String> fieldNames;
    /**
     * Types of all fields. Extracted from schema - window's meta data.
     */
    private ArrayList<FieldTypes> fieldTypes;

    public DfESPConsumer(DfESPEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        endpointUri = endpoint.getEndpointUri();
    }

    @Override
    public final void dfESPGDpublisherCB_func(final clientGDStatus arg0, final long arg1, final Object arg2) {
        LOG.error("should not be called - i am a subscriber not an publisher.");
    }

    @Override
    public final void dfESPpubsubErrorCB_func(final clientFailures failure, final clientFailureCodes failureCode,
                                              final Object arg2) {
        LOG.error("Subscriber/Consumer received error from cep: Error " + failure.name() + " with code "
                  + failureCode.name());
        if (failureCode == clientFailureCodes.pubsubCode_READFAILED
            || failureCode == clientFailureCodes.pubsubCode_WRITEFAILED) {
            LOG.info("Received " + failureCode.name()
                     + " from cep engine. Will raise engine down flag. Further attempts to publish will fail.");
            DfESPEndpoint.engineDown();
        }

    }

    @Override
    public final void dfESPsubscriberCB_func(final dfESPeventblock eventBlock, final dfESPschema schema,
                                             final Object arg2) {
        int blockSize = eventBlock.getSize();
        LOG.trace("Received block from " + endpointUri + " with " + blockSize + " events");
        // create exchange for each event
        endpoint.countBlockReceived();
        for (int i = 0; i < blockSize; i++) {
            Exchange exchange = endpoint.createExchange();
            dfESPevent event = eventBlock.getEvent(i);
            // filter by opcode, if not set process all events
            if (endpoint.getSubscribeMode() == null || endpoint.getSubscribeMode().equals(event.getOpcode())) {
                try {
                    List<EspDataItem> espData = createDataItem(event.toStringCSV(schema, true, false));
                    exchange.getIn().setBody(espData);
                } catch (dfESPException e) {
                    throw new IllegalArgumentException("Problem on serializing event, cause: ", e);
                }
                try {
                    getProcessor().process(exchange);
                    endpoint.countEventReceived();
                } catch (Exception e) {
                    throw new IllegalStateException("Error on processing exchange from esp: ", e);
                }
            }
        }
    }

    /**
     * Creates an {@link EspDataItem} from given esp-csv string.
     *
     * @param stringCSV
     *            data received from esp as csv string
     * @return List of single data items including meta data
     */
    private List<EspDataItem> createDataItem(final String stringCSV) {
        // cuts off I,N:, include trailing empty strings
        String[] dataArray = stringCSV.substring(INDEX_TO_CUT).split(",", -1);

        List<EspDataItem> result = new ArrayList<>();
        if (!(fieldNames.size() == dataArray.length)) {
            throw new AssertionError(
                                     "Length of data array should always match length of list of fieldnames. Length of data array: "
                                             + dataArray.length + ", number of fields given by meta data: "
                                             + fieldNames.size());
        }
        for (int i = 0; i < dataArray.length; i++) {
            result.add(new EspDataItem(i, fieldNames.get(i), dataArray[i], fieldTypes.get(i)));
        }
        return result;
    }

    @Override
    protected final void doStart() throws Exception {
        super.doStart();
        LOG.debug("Subscriber connecting to " + endpointUri);
        handler = new dfESPclientHandler();

        handler.init(Level.WARNING);

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
            dfESPschema schema = new dfESPschema(schemaVector.get(0));
            fieldNames = schema.getNames();
            fieldTypes = schema.getTypes();
        } catch (dfESPException e) {
            throw new IllegalStateException("Problem on creating schema, cause: ", e);
        }

        // sets queue size of window within esp engine. engine will ballance
        // between in and output - if block is set to true.
        if (!handler.subscriberMaxQueueSize(getEndpoint().getEndpointUri(), MAX_QUEUE_SIZE_CEP, true)) {
            throw new IllegalStateException("Failed to set max queue size for subscriber.");
        }
        client = handler.subscriberStart(getEndpoint().getEndpointUri() + "?snapshot=true", this, 0);
        if (client == null) {
            throw new IllegalStateException("Client object creation for connection to CEP engine not possible");
        }
        try {
            handler.connect(client);
        } catch (SocketException | UnknownHostException e) {
            throw new IllegalStateException("Can't connect to esp engine.");
        }
    }

}

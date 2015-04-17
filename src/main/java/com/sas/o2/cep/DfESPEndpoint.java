package com.sas.o2.cep;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.api.management.ManagedAttribute;
import org.apache.camel.api.management.ManagedOperation;
import org.apache.camel.api.management.ManagedResource;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sas.esp.api.server.event.EventOpcodes;


/**
 * Represents a dfESP endpoint.
 */

@UriEndpoint(scheme = "dfESP")
@ManagedResource(description = "DfESP endpoint")
public class DfESPEndpoint extends DefaultEndpoint {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(DfESPEndpoint.class);

    /**
     * Mode for sending data - needed by producer.
     */
    @UriParam
    private Mode mode;

    /**
     * When true and exchange property "CamelSplitComplete" is set to true, cep
     * waits till the input is loaded.
     */
    @UriParam
    private boolean quiesce;

    /**
     * @return the mode
     */
    public final Mode getMode() {
        return mode;
    }

    /**
     * @param mode
     *            mode for inserting data
     */
    public final void setMode(final Mode mode) {
        this.mode = mode;
    }

    /**
     * @param quiesce
     *            the quiesce to set
     */
    public final void setQuiesce(boolean quiesce) {
        this.quiesce = quiesce;
    }

    public boolean isQuiesce() {
        return quiesce;
    }

    /**
     * Data received from esp engine is filtered by this opcode. E.g. insert
     * means that only insert events are processed. If null all events will be
     * processed.
     */
    @UriParam
    private EventOpcodes subscribeMode = null;

    /**
     * @return the subscribeMode
     */
    public final EventOpcodes getSubscribeMode() {
    	
        return subscribeMode;
    }

    /**
     * @param subscribeMode
     *            the subscribeMode to set
     */
    public final void setSubscribeMode(final EventOpcodes subscribeMode) {
        this.subscribeMode = subscribeMode;
    }

    /**
     * Consumer or producer can set this to true if the get called on
     * dfESPpubsubErrorCB_func. The other side could then use this to throw
     * exception if process is called.
     */
    private static volatile Boolean engineDown = false;

    /**
     * Uri for this endpoint.
     */
    private String uri;

    /**
     *
     * @return uri of this endpoint.
     */
    @ManagedAttribute
    public final String getUri() {
        return uri;
    }

    /**
     * Counts event blocks sent.
     */
    private long blocksSent = 0;

    /**
     *
     * @return Blocks sent by this endpoint.
     */
    @ManagedAttribute(description = "Blocks(=files) sent by this endpoint.")
    public final long getBlocksSent() {
        return blocksSent;
    }

    /**
     * Counts events sent.
     */
    private long eventsSent = 0;

    /**
     *
     * @return events sent.
     */
    @ManagedAttribute(description = "Events(=lines) sent by this enpoint.")
    public final long getEventsSent() {
        return eventsSent;
    }

    /**
     * Resets blocks sent and events sent counter.
     */
    @ManagedOperation(description = "Resets blocks sent and events sent counter.")
    public final void resetSentCounter() {
        blocksSent = 0;
        eventsSent = 0;
    }

    /**
     * Adds one to block sent count.
     */
    protected final void countBlockSent() {
        blocksSent++;
    }

    /**
     * Adds one to events sent count.
     */
    protected final void countEventSent() {
        eventsSent++;
    }

    /**
     * Counts blocks received by this endpoint.
     */
    private long blocksReceived = 0;

    /**
     *
     * @return Blocks received by this endpoint.
     */
    @ManagedAttribute(description = "Blocks received by this endpoint.")
    public final long getBlocksReceived() {
        return blocksReceived;
    }

    /**
     * Adds one to blocks received count.
     */
    protected final void countBlockReceived() {
        blocksReceived++;
    }

    /**
     * Counts events received by this endpoint.
     */
    private long eventsReceived = 0;

    /**
     *
     * @return Events received by this endpoint.
     */
    @ManagedAttribute(description = "Events received by this endpoint")
    public final long getEventsReceived() {
        return eventsReceived;
    }

    /**
     * Adds one to event received count.
     */
    protected final void countEventReceived() {
        eventsReceived++;
    }

    /**
     * Resets the counter for blocks and events received.
     */
    @ManagedOperation(description = "Resets the counter for blocks and events received.")
    public final void resetReceivedCounter() {
        blocksReceived = 0;
        eventsReceived = 0;
    }

    /**
     * @return true if engine down. will be unchanged until restart.
     */
    protected static final Boolean isEngineDown() {
        return engineDown;
    }

    /**
     * Sets engine down to true.
     */
    protected static final void engineDown() {
        engineDown = true;
    }

    /**
     * Empty constructor - needed by camel?!
     */
    public DfESPEndpoint() {
    }

    /**
     * To create an endpoint via java dsl.
     *
     * @param uri
     *            from or to
     * @param component
     *            current component.
     */
    public DfESPEndpoint(final String uri, final DfESPComponent component) {
        super(uri, component);
        this.uri = uri;

        // getCamelContext().getManagementNameStrategy().setNamePattern("myName");
    }

    /**
     * Uses only last part of uri (after last /) as key for endpoint.
     *
     * @return part of uri after last '/' (or original uri if this is empty)
     */
    @Override
    public final String getEndpointKey() {
        String originalKey = super.getEndpointKey();
        String lastPart = originalKey.substring(originalKey.lastIndexOf('/') + 1);
        if (lastPart != null && lastPart.trim().length() > 0) {
            return lastPart;
        } else {
            return originalKey;
        }
    }

    @Override
    public final Producer createProducer() throws Exception {
        return new DfESPProducer(this);
    }

    @Override
    public final Consumer createConsumer(final Processor processor) throws Exception {
        return new DfESPConsumer(this, processor);
    }

    @Override
    public final boolean isSingleton() {
        return true;
    }

    /**
     *
     * @return url of dfesp project (query and window part cut off)
     */
    public String getProjectName() {
        String[] result = this.uri.split("/");
        String project = result[0] + "//" + result[2] + "/" + result[3];
        LOG.debug("getProjectName project = " + project);
        return project;
    }
}

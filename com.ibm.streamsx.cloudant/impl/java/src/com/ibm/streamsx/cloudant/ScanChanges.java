//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant;

import org.apache.log4j.Logger;

import com.cloudant.client.api.Changes;
import com.cloudant.client.api.Database;
import com.cloudant.client.api.model.ChangesResult;
import com.google.gson.JsonObject;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.cloudant.client.ChangesReader;
import com.ibm.streamsx.cloudant.client.Client;
import com.ibm.streamsx.cloudant.client.ClientImpl;
import com.ibm.streamsx.cloudant.client.Configuration;
import com.ibm.streamsx.cloudant.client.ReaderResult;
import com.ibm.streamsx.cloudant.util.DefaultSequenceManager;
import com.ibm.streamsx.cloudant.util.SequenceManager;
import com.ibm.streamsx.cloudant.util.StreamsHelper;

/**
 * Scan changes from the Cloudant database, using the _changes API
 *
 */
@PrimitiveOperator(name="ScanChanges", namespace="com.ibm.streamsx.cloudant",description=ScanChanges.operatorDescription)
@OutputPorts({@OutputPortSet(description=ScanChanges.oport0Description, cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
public class ScanChanges extends AbstractCloudantOperator
{
	// parameter related members ---------------------------------------------------------------
	
	// the name of the output attr to get the doc id
	private static final String DOCID_ATTR_PARAM = "documentIdAttribute";
	private static final String DOCID_ATTR_DEFAULT = "id";
	private String documentIdAttribute = DOCID_ATTR_DEFAULT;

	// the name of the output attr to get the doc data
	private static final String DOCISDELETED_ATTR_PARAM = "isDeletedAttribute";
	private static final String DOCISDELETED_ATTR_DEFAULT = "isDeleted";
	private String isDeletedAttribute = DOCISDELETED_ATTR_DEFAULT;

	// optional output attributes 
	
	// the name of the output attribute to get the doc data
	private static final String DOCDATA_ATTR_PARAM = "documentDataAttribute";
	private String documentDataAttribute = null;
	
	// the name of the output attr to get the doc revision
	private static final String DOCREV_ATTR_PARAM = "documentRevisionAttribute";	
	private String documentRevisionAttribute = null;

	// the name of the output attr to get the doc revision
	private static final String DOCSEQNO_ATTR_PARAM = "documentSequenceValueAttribute";	
	private String documentSequenceValueAttribute = null;
	
	// parameters determining how to get the inital sequence value 
	
	/**
	 * Where to get the first sequence number from
	 *
	 */
	public enum StartSequenceValueMode {
		all,
		now,
		fromParameter,
		fromDatabase
	}
	
	// the startmode
	private StartSequenceValueMode startSequenceValueMode = StartSequenceValueMode.all;
	
	// optional start sequence value 
	private String startSequenceValue = null;
	
	// optional start sequence value storage document id
	// if this is set, the SequenceManager will track the sequence value on application start/stop
	private String sequenceValueDocumentId = null;

	// reset the sequence value document on application start ? (good for testing) 
	private boolean resetSequenceValueDocument = false;

	// TODO add filter 
	// document filter to set on the _changes API 
	private String documentFilter = null;
	
	// internal members -----------------------------------------------------------------------
	
	// the client, encapsulating all Cloudant interaction 
	private Client client = null;
	
	// Logger for tracing.
    private static Logger logger = Logger.getLogger(ScanChanges.class.getName());

    // the last sequence number 
    private String lastSequenceValue = null;

    // thread for producing tuples
    private Thread processThread;

	// the changes feed from Cloudant
	private ChangesReader reader = null;
	
	// the manager used to get/store sequence values across job restarts
	private SequenceManager seqMgr = null;

    /**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void initialize(OperatorContext context) throws Exception {
    	
        // Must call super.initialize(context) to correctly setup an operator.
        super.initialize(context);
        logger.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
        
        // check runtime sequence value config
        if (!runtimeCheckerSeq()) {
        	throw new RuntimeException("Invalid SequenceValue configuration found");
        }

        /*
         * Create the thread at initialize time but do not start it.
         * The thread will be started by startProcessing() which will
         * be called at allPortsReady() time if no delay is specified,
         * otherwise it will be called once the delay has expired.
         */
        processThread = getOperatorContext().getThreadFactory().newThread( new Runnable() {
        	@Override
        	public void run() {
        		try {
        			process();
        		} catch (Exception e) {
        			throw new RuntimeException(e);
        		}
        	}
        });
        
        /*
         * Set the thread not to be a daemon to ensure that the SPL runtime
         * will wait for the thread to complete before determining the
         * operator is complete.
         */
        processThread.setDaemon(false);

        // create the client and check the configuration
        Configuration config = getConfiguration();
        
        // bail out if the configuration has an IAMApiKey set
        if (config.getIAMApiKey() != null) {
        	logger.fatal("ScanChanges operator does not support IAM authentication so far.");
        	throw new RuntimeException("Unsupported configuration");
        }
        
        client = new ClientImpl(config);
        client.setLogger(logger);
        if (!client.validateConfiguration()) {
        	logger.fatal("Client configuration is invalid: " + config.toString());
        	throw new RuntimeException("Invalid Client configuration");
        }
        
        // initiate connection to cloudant
        if (!client.connect()) {
        	logger.fatal("Cannot connect to database, configuration: " + config.toString());
        	throw new RuntimeException("Connection error");
        }
        
        logger.trace("Operator " + context.getName() + " Cloudant client has been initialized" + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
    }

    /**
     * Notification that initialization is complete and all input and output ports 
     * are connected and ready to receive and submit tuples.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void allPortsReady() throws Exception {
        // This method is commonly used by source operators. 
        // Operators that process incoming tuples generally do not need this notification. 
        OperatorContext context = getOperatorContext();
        
        // start thread that receives database changes
        processThread.start();
        
        logger.trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
    }

    /**
     * Ingest source data 
     */
    public void process() throws Exception {
    	
    	final StreamingOutput<OutputTuple> out = getOutput(0);
    	long changeCount = 0;

    	// initialize Sequence value manager
    	String since = getSince(); 
    	
    	// figure out the options for the output tuple
    	boolean getDocument = documentDataAttribute == null ? false : true;
    	boolean getRevision = documentDataAttribute == null ? false : true;
    	boolean getSeqNo = documentSequenceValueAttribute == null ? false : true;
    	
    	// create changes reader 
    	reader = client.getReader(false, since, getDocument);
    	if (null == reader) {
    		logger.fatal("CANNOT_CREATE_READER");
			throw new RuntimeException("ChangesReader Error");
    	}
    	
    	while (!Thread.interrupted()) {
    		
    		// this call blocks for at most 2 seconds (the readerHeartbeatInterval) 
    		ReaderResult change = reader.getNext();
    		
    		if (change.hasChange()) {
    			changeCount++;

    			// remember sequence value
    			lastSequenceValue = change.getSeq();
    			logger.trace("Change received, sequenceValue=" + lastSequenceValue);
    			
    			String docId = change.getId();
    			// dont output anything if we got the update for the sequence manager doc
            	if (sequenceValueDocumentId != null && sequenceValueDocumentId.equals(docId)) {
            		continue;
            	}
    			
            	// add mandatory output attributes
            	OutputTuple tuple = out.newTuple();
            	tuple.setString(documentIdAttribute, docId);
            	boolean isDeleted = change.isDeleted();
            	tuple.setBoolean(isDeletedAttribute, isDeleted);

            	// optionally set sequence value as string
            	if (getSeqNo) {
            		tuple.setString(documentSequenceValueAttribute, lastSequenceValue);
            	}
            	
            	// optionally set document
            	if (getDocument) {
            		String doc = change.getDoc();
            		tuple.setString(documentDataAttribute, doc);
            	}

            	// optionally set the first _rev
            	if (getRevision) {
            		String rev = change.getRev();
            		tuple.setString(documentRevisionAttribute, rev);
            	}

            	// submit result
            	out.submit(tuple);
            	
            	// acknowledge seqNo, every 1000 changes
            	if (null != seqMgr) {
            		if (changeCount % 1000 == 0) {
            			changeCount = 0;
            			seqMgr.updateOrCreate(lastSequenceValue);
            		}
            	}

    		} else {
    			// heartbeat or error received
    	    	logger.trace("Reader heartbeat received");    			
    		}
    	}
    	
    	logger.error("Source thread interrupted, proccessing stoppped, last sequenceValue=" + lastSequenceValue);
    	
    }
    
    /*
     * get the first sequence number  
     * @return
     */
    private String getSince() {
    	String initValue = null;
    	if (StartSequenceValueMode.all == startSequenceValueMode) {
    		initValue = "0";
    	}
    	if (StartSequenceValueMode.now == startSequenceValueMode) {
    		// TODO get latest seq value via CLient API
    		initValue = "now";
    	}
    	if (StartSequenceValueMode.fromParameter == startSequenceValueMode) {
    		initValue = startSequenceValue;
    	}
    	
    	String realValue = initValue;
    	
    	// use the sequence value manager
    	if (sequenceValueDocumentId != null) {
    		seqMgr = new DefaultSequenceManager(client, sequenceValueDocumentId);
    		
    		// initialize
    		if (!seqMgr.initialize()) {
    			logger.fatal("CANNOT_INIT_SEQMANAGER");
    			throw new RuntimeException("SequenceManager Error");
    		}
    		
    		if (StartSequenceValueMode.fromDatabase == startSequenceValueMode) {
    			// we have no fallback sequence value, so only read is allowed 
    			realValue = seqMgr.read();
    		} else {
    			if (resetSequenceValueDocument) {
    				// we dont care about the actual value in the database, just overwrite it with the given sequence number
    				seqMgr.updateOrCreate(initValue);
    			} else {
    				// read an existing value, or if the entry does not exist create a new one with the initial value
    				realValue = seqMgr.readOrCreate(initValue);
    			}
    		}
    		
    		// this may happen if the read or readOrCreate call returns null
    		if (null == realValue) {
    			logger.fatal("CANNOT_GET_SEQVALUE");
    			throw new RuntimeException("SequenceManager Error");
    		}
    	}

    	return realValue;
	}

	/**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        // stop the feed if active
        reader.stopReading();

        // acknowledge seqNo
    	if (null != seqMgr) {
    		seqMgr.updateOrCreate(lastSequenceValue);
    	}

        // shutdown cloudant client
        if (null != client) {
        	client.disconnect();
        }

        // Must call super.shutdown()
        super.shutdown();
    }
    
    // sequence value related parameters -------------------------------------------------------------------------------------------
    
	@Parameter(
		name="startSequenceValueMode", optional=true,
		description="Specify how the operator should start the replication (parameter 'since' in the CLoudant _changes API). The sequence value to start with can be specified in 4 ways."
		+ "'all' use 0 as since value. This will fetch every change ever made to the database. Be cautious as this can result in overloading the application."
		+ "'now' use the latest sequence value from the database. This will fetch changes occured after starting the Streams application."
		+ "'fromParameter' you need to supply the desired sequence value in the operator parameter 'startSequenceValue'."
		+ "'fromDatabase' the sequence value is stored in the source database itself, and managed by this operator. You need to supply the id of the document that will be used."
		+ "The default value is 'all'"
	)
	public void setStartSequenceValueMode(StartSequenceValueMode startSequenceValueMode) {
		this.startSequenceValueMode = startSequenceValueMode;
	}

	@Parameter(
		name="startSequenceValue", optional=true,
		description="Specify the initial sequence value to use (parameter 'since' in the CLoudant _changes API)."
		+ "parameter 'startSequenceValueMode' needs to be set to 'fromParameter' for this parameter to take effect."
	)
	public void setStartSequenceValue(String startSequenceValue) {
		this.startSequenceValue = startSequenceValue;
	}

	@Parameter(
		name="sequenceValueDocumentId", optional=true,
		description="Specify the id of a document in the Cloudant database that is used to manage the sequence value (parameter 'since' in the CLoudant _changes API)."
		+ "If this parameter is set, the operator will track the sequence value in this document. On startup the initial sequence value is retrieved from this document."
		+ "If the document does not exists in the datbase, it is created and populated with the sequence number as determined via the 'startSequenceValueMode' parameter."
		+ "If the document does not exist and the parameter 'startSequenceValueMode' is set to 'fromDatabase' this is treated as an error and the operator throws an exception."
		+ "On shutdown of the application the latest sequence value processed is stored in this document, so the replication can be continued at that point after an application restart."
	)
	public void setSequenceValueDocumentId(String sequenceValueDocumentId) {
		this.sequenceValueDocumentId = sequenceValueDocumentId;
	}
	
	@Parameter(
		name="resetSequenceValueDocument", optional=true,
		description="If this value is set to 'true' the document used to track the sequence value is cleared out after application start."
		+ "This is useful for debugging or if you want to start replication from scratch. Alternatively, the document can be deleted manually from the database, as this has the same effect."
	)
	public void setResetSequenceValueDocument(boolean resetSequenceValueDocument) {
		this.resetSequenceValueDocument = resetSequenceValueDocument;
	}

	@Parameter(
		name="documentFilter", optional=true,
		description="Specify a JSON string that contains a filter for the documents to retrieve during scanning changes."
	)
	public void setDocumentFilter(String documentFilter) {
		this.documentFilter = documentFilter;
	}

	/**
	 * check the parameter value combinations for the sequence value handling at runtime 
	 */
    public boolean runtimeCheckerSeq() {
		
    	if (resetSequenceValueDocument && (sequenceValueDocumentId == null)) {
			logger.fatal("RESETDOC_TRUE_BUT_SEQVAL_DOCID_MISSING");
			return false;
    	}
    	
		if (startSequenceValueMode == StartSequenceValueMode.fromDatabase) {
			if (null == sequenceValueDocumentId) {
				logger.fatal("MODE_DB_BUT_SEQVAL_DOCID_MISSING");
				return false;
			}
			if (true == resetSequenceValueDocument) {
				logger.fatal("MODE_DB_AND_SEQVAL_RESET_SPECIFIED");
				return false;
			}
		}

		if (startSequenceValueMode == StartSequenceValueMode.fromParameter) {
			if (null == startSequenceValue) {
				logger.fatal("MODE_PARAM_BUT_SEQVAL_PARAM_MISSING");
				return false;
			}
		} else {
			if (null != startSequenceValue) {
				logger.warn("PARAM_SEQVAL_IGNORED");
				return true;
			}
		}

		return true;
	}	
	
    // operator specific in/out parameters -----------------------------------------------------------------------------------------

	@Parameter(
		name=DOCID_ATTR_PARAM, optional=true,
		description="This parameter specifies the name of the attribute in the output tuple that will receive the id of the changed docuemnt (JSON field _id). It is of type rstring. If not specified an attribute named 'id' is expected in the output tuple and will get the value."
	)
	public void setDocumentIdAttribute(String documentIdAttribute) {
		this.documentIdAttribute = documentIdAttribute;
	}
    
	@Parameter(name=DOCISDELETED_ATTR_PARAM, optional=true,
		description="This parameter specifies the name of the attribute in the output tuple that will receive the flag if the changed docuemnt was deleted. It is of type boolean. If not specified an attribute named 'iDeleted' is expected in the output tuple and will get the value."
	)
	public void setIsDeletedAttribute(String isDeletedAttribute) {
		this.isDeletedAttribute = isDeletedAttribute;
	}
	
	@Parameter(name=DOCDATA_ATTR_PARAM, optional=true,
		description="This parameter specifies the attribute name of the output tuple that gets the documents JSON content (type rstring). If not specified, the document is not included in the output tuple."
	)
	public void setDocumentDataAttribute(String documentDataAttribute) {
		this.documentDataAttribute = documentDataAttribute;
	}

	@Parameter(name=DOCREV_ATTR_PARAM, optional=true,
		description="This parameter specifies the attribute name of the output tuple that gets the documents revision number (type rstring).  If not specified, the revision is not included in the output tuple."
	)
	public void setDocumentRevisionAttribute(String documentRevisionAttribute) {
		this.documentRevisionAttribute = documentRevisionAttribute;
	}

	@Parameter(name=DOCSEQNO_ATTR_PARAM, optional=true,
		description="This parameter specifies the attribute name of the output tuple that gets the last sequnce number fetched from the changes source (type rstring). It is optional. This number can be used to restart the replication from a certain point."
	)
	public void setDocumentSequenceValueAttribute(String documentSequenceValueAttribute) {
		this.documentSequenceValueAttribute = documentSequenceValueAttribute;
	}

	@ContextCheck(compile = true)
    public static void compiletimeChecker(OperatorContextChecker checker) {
		StreamsHelper.validateOutputAttribute(checker, DOCID_ATTR_PARAM, DOCID_ATTR_DEFAULT, MetaType.RSTRING);
		StreamsHelper.validateOutputAttribute(checker, DOCISDELETED_ATTR_PARAM, DOCISDELETED_ATTR_DEFAULT, MetaType.BOOLEAN);
	}	
	
	@ContextCheck(compile = false, runtime = true)
    public static void runtimeChecker(OperatorContextChecker checker) {
		StreamsHelper.validateOutputAttributeRuntime(checker, DOCID_ATTR_PARAM, DOCID_ATTR_DEFAULT, MetaType.RSTRING);
		StreamsHelper.validateOutputAttributeRuntime(checker, DOCISDELETED_ATTR_PARAM, DOCISDELETED_ATTR_DEFAULT, MetaType.BOOLEAN);

		// optional output attributes
		StreamsHelper.validateOutputAttributeRuntime(checker, DOCDATA_ATTR_PARAM, null, MetaType.RSTRING);
		StreamsHelper.validateOutputAttributeRuntime(checker, DOCREV_ATTR_PARAM, null, MetaType.RSTRING);
		StreamsHelper.validateOutputAttributeRuntime(checker, DOCSEQNO_ATTR_PARAM, null, MetaType.RSTRING);
	}
	
	// documentation ------------------------------------------------------------------------------------------
	
	// TODO improve
	public static final String operatorDescription =
		"This operator uses the Cloudant changes API to get all changes made to documents in the database.";

	// TODO improve
	public static final String oport0Description =
		"This output port gets one tuple for each database change, containing the docuemnt id and optionally the content.";
   
}

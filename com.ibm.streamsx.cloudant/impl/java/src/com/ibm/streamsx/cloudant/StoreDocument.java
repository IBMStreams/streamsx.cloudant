//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.DefaultAttribute;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.cloudant.client.Client;
import com.ibm.streamsx.cloudant.client.ClientImpl;
import com.ibm.streamsx.cloudant.client.Configuration;
import com.ibm.streamsx.cloudant.client.StoreResult;
import com.ibm.streamsx.cloudant.util.StreamsHelper;

/**
 * Store a document into the Cloudant database 
 */
@PrimitiveOperator(name="StoreDocument", namespace="com.ibm.streamsx.cloudant",description=StoreDocument.operatorDescription)
@InputPorts({@InputPortSet(id="0", description=StoreDocument.iport0Description, cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description=StoreDocument.oport0Description, cardinality=1, optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Preserving, windowPunctuationInputPort="0")})
public class StoreDocument extends AbstractCloudantOperator
{
	
	// parameter related members ---------------------------------------------------------------
	
	// the name of attribute containing the document to store
	private static final String DOC_ATTR_PARAM = "documentAttribute";
	private static final String DOC_ATTR_DEFAULT = "document";
	private TupleAttribute<Tuple, String> documentAttribute = null;

	// the name of the attribute containing the id of the document to store (optional)
	private static final String DOCID_ATTR_PARAM = "documentIdAttribute";
	private TupleAttribute<Tuple, String> documentIdAttribute = null;

	// the name of the attribute containing the revision of the document to store (optional)
	private static final String DOCREV_ATTR_PARAM = "documentRevisionAttribute";
	private TupleAttribute<Tuple, String> documentRevisionAttribute = null;

	// internal members -----------------------------------------------------------------------

	// the client, encapsulating all Cloudant interaction 
	private Client client = null;
	
	// Logger for tracing.
    private static Logger logger = Logger.getLogger(StoreDocument.class.getName());
    
    // indicate if we have an output port
    private boolean doOutput = false;
    
    // methods --------------------------------------------------------------------------------
    
    /**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
        logger.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());

        // check if we need to do output
        if(context.getStreamingOutputs().size() == 1) {
        	doOutput = true;
        }
        
        // create the client and check the configuration
        Configuration config = getConfiguration();
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

        // all went well 
        logger.trace("Operator " + context.getName() + " Cloudant client has been initialized" + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
    }

    /**
     * Notification that initialization is complete and all input and output ports 
     * are connected and ready to receive and submit tuples.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void allPortsReady() throws Exception {
        OperatorContext context = getOperatorContext();
        logger.trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
    }

    /**
     * Process an incoming tuple that arrived on the specified port.
     * @param stream Port the tuple is arriving on.
     * @param tuple Object representing the incoming tuple.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {

    	String errorCode = "";

        // get document data  from input tuple
        String docData = documentAttribute.getValue(tuple);
        
        // get id and rev if set
        String idValue = null;
        if (documentIdAttribute != null) {
        	idValue = documentIdAttribute.getValue(tuple);
        }
        String revValue = null;
        if (documentRevisionAttribute != null) {
        	revValue = documentRevisionAttribute.getValue(tuple);
        }
        
		// Create new output tuple and copy over matching attributes
    	StreamingOutput<OutputTuple> outStream = null;
        OutputTuple outTuple = null;
        if (doOutput) {
        	outStream = getOutput(0);
        	outTuple = outStream.newTuple();
    		outTuple.assign(tuple);
        }

        // Store document in database
        StoreResult res = client.storeDocument(docData, idValue, revValue);
        if (!res.isSuccess()) {
        	errorCode = res.getErrorMessage();
        	logger.error("Error during store: " + errorCode);
        } else {
        	logger.trace("Successfully stored document");
        }
        
        // submit result, if output port is configured
        if (doOutput) {
        	// set error message
        	setErrorCode(outTuple,errorCode);
        
        	// submit result tuple and punctuation if needed
        	outStream.submit(outTuple);
        	if (!isPreservePunctuation()) {
        		outStream.punctuate(Punctuation.WINDOW_MARKER);
        	}
        }
    }
    
    /**
     * Process an incoming punctuation that arrived on the specified port.
     * @param stream Port the punctuation is arriving on.
     * @param mark The punctuation mark
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void processPunctuation(StreamingInput<Tuple> stream, Punctuation mark) throws Exception {
    	if (mark == Punctuation.WINDOW_MARKER) {
    		if (doOutput && isPreservePunctuation()) {
    			super.processPunctuation(stream, mark);
    		}
    	}
    	else {
    		super.processPunctuation(stream, mark);
    	}
    }

    /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        // shutdown cloudant client
        if (null != client) {
        	client.disconnect();
        }
        // Must call super.shutdown()
        super.shutdown();
    }
    
    // operator specific parameter operations ------------------------------------------------------------------------

	@Parameter(
		name=DOC_ATTR_PARAM, optional=true,
		description="This parameter specifies the attribute of the input tuple that contains the document to store in Cloudant as JSON string. "
		+ "The attribute must be of type 'rstring'. If this parameter is not set, an attribute named 'document' is expected in the input stream."
	)
	@DefaultAttribute(DOC_ATTR_DEFAULT)
	public void setdocumentAttribute(TupleAttribute<Tuple,String> documentAttribute) {
		this.documentAttribute = documentAttribute;
	}

	@Parameter(
		name=DOCID_ATTR_PARAM, optional=true,
		description="This parameter specifies an attribute of the input tuple that contains the id of the document to store in Cloudant (JSON field '_id')."
		+ "The attribute must be of type 'rstring'. If specified it overrides the _id value given in the document to store. If this parameter is not set, the content of the document may contain an _id attribute."
		+ "If no id is specified Cloudant will assign a unique id for the new document on its own."
	)
	public void setdocumentIdAttribute(TupleAttribute<Tuple,String> documentIdAttribute) {
		this.documentIdAttribute = documentIdAttribute;
	}

	@Parameter(
		name=DOCREV_ATTR_PARAM, optional=true,
		description="This parameter specifies an attribute of the input tuple that contains the revision of the document to store in Cloudant (JSON field '_rev')."
		+ "The attribute must be of type 'rstring'. If specified it overrides the _rev value given in the document to store. If this parameter is not set, the content of the document may contain an _rev attribute."
		+ "It is an error to specify a revision but no id, because cloudant will assign an id and revision in this case. If the revision does not match the latest revision of the document a docuemnt conflict error is raised."
	)
	public void setdocumentRevisionAttribute(TupleAttribute<Tuple,String> documentRevisionAttribute) {
		this.documentRevisionAttribute = documentRevisionAttribute;
	}
	
	@ContextCheck(compile = true)
    public static void compiletimeChecker(OperatorContextChecker checker) {
		StreamsHelper.validateInputAttribute(checker, DOC_ATTR_PARAM, DOC_ATTR_DEFAULT, MetaType.RSTRING);
	}	
	
	// documentation ------------------------------------------------------------------------------------------
	
	// TODO improve
	public static final String operatorDescription =
		"This operator stores JSON strings in the configured Cloudant database."
		+ "The JSON string must be provided as rstring attribute in the input tuple. The attribute name is configrable."
		+ "Optionally the id and revision can be specified via input attributes. If present, any id and revision information in the document is overwritten."
		+ "If the docuement is already present in the database, it is overwritten. Otherwise it is created.";

	// TODO improve
	public static final String iport0Description =
		"The schema in this port contains attributes for the document, its id and its revision.";

	// TODO improve
	public static final String oport0Description =
		"This optional output port can contain an attribute that receives an error code for the store operation."
		+ "Matching attributes are copied from the input port to this output port.";

}

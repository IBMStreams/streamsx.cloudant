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
import com.ibm.streamsx.cloudant.client.FetchResult;
import com.ibm.streamsx.cloudant.util.StreamsHelper;

/**
 * Fetch a document from the Cloudant database 
 */
@PrimitiveOperator(name="FetchDocument", namespace="com.ibm.streamsx.cloudant",description=FetchDocument.operatorDescription)
@InputPorts({@InputPortSet(id="0", description=FetchDocument.iport0Description, cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description=FetchDocument.oport0Description, cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Preserving, windowPunctuationInputPort="0")})
public class FetchDocument extends AbstractCloudantOperator
{
	
	// parameter related members ----------------------------------------------------------------------
	
	// the name of the attribute that contains the id of the document to fetch
	private static final String DOCID_ATTR_PARAM = "documentIdAttribute";
	private static final String DOCID_ATTR_DEFAULT = "id";
	private TupleAttribute<Tuple, String> documentIdAttribute = null;

	// the name of the attribute that contains the revision of the document to fetch
	private static final String DOCREV_ATTR_PARAM = "documentRevisionAttribute";
	private TupleAttribute<Tuple, String> documentRevisionAttribute = null;

	// the name of the output attribute to get the doc data
	private static final String DOCDATA_ATTR_PARAM = "documentDataAttribute";
	private static final String DOCDATA_ATTR_DEFAULT = "document";
	private String documentDataAttribute = DOCDATA_ATTR_DEFAULT;
	
	// internal members -------------------------------------------------------------------------------

	// the client, encapsulating all Cloudant interaction 
	private Client client = null;
	
	// Logger for tracing.
    private static Logger logger = Logger.getLogger(FetchDocument.class.getName());
    
    // methods ----------------------------------------------------------------------------------------
    
    /**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
        logger.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
        
        // create the client and check the configuration
        Configuration config = getConfiguration();
        
        // use a timeout of 5 seconds
        config.setReadTimeout((long) 5);
        
        // create client
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
        StreamingOutput<OutputTuple> outStream = getOutput(0);

        // get doc id from input tuple
        String idName = documentIdAttribute.getValue(tuple);
        
        // get revision if set
        String revName = null;
        if (documentRevisionAttribute != null) {
        	revName = documentRevisionAttribute.getValue(tuple);
        }
        
		// Create new output tuple and copy over matching attributes
        OutputTuple outTuple = outStream.newTuple();        
		outTuple.assign(tuple);

        // read the doc from database
		FetchResult res = client.fetchDocument(idName, revName);
		if (res.isValid()) {
			outTuple.setString(documentDataAttribute, res.getResult());
			logger.trace("Successfully fetched document from database, id: " + idName);
		} else {
			outTuple.setString(documentDataAttribute, "");
			errorCode = res.getErrorMessage();
			if (res.getStatus() == FetchResult.Status.NOTFOUND) {
				logger.trace("Document not found, id: " + idName);
			} else {
				logger.fatal("Error during fetch: " + res.getStatus().name() + " , message: " + errorCode + " id: " + idName);
			}
		}
		
        // set error message
        setErrorCode(outTuple,errorCode);
        
		// submit result tuple and punctuation if needed
        outStream.submit(outTuple);
        if (!isPreservePunctuation()) {
        	outStream.punctuate(Punctuation.WINDOW_MARKER);
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
    		if (isPreservePunctuation()) {
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
        logger.trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        // shutdown cloudant client
        if (null != client) {
        	client.disconnect();
        }
        super.shutdown();
    }
    
    // operator specific parameter operations ------------------------------------------------------------------------

	@Parameter(
		name=DOCID_ATTR_PARAM, optional=true,
		description="This parameter specifies the attribute of the input tuple that contains the id of the document in Cloudant (JSON field '_id'). The attribute must be of type 'rstring'. If this parameter is not set, an attribute named 'id' is expected in the input stream."
	)
	@DefaultAttribute(DOCID_ATTR_DEFAULT)
	public void setdocumentIdAttribute(TupleAttribute<Tuple,String> documentIdAttribute) {
		this.documentIdAttribute = documentIdAttribute;
	}

	@Parameter(
		name=DOCREV_ATTR_PARAM, optional=true,
		description="This parameter specifies the attribute of the input tuple that contains the revision of the document in Cloudant (JSON field '_rev'). The attribute must be of type 'rstring'. If this parameter is not set, only the id attribute used for fetching the document."
	)
	public void setdocumentRevisionAttribute(TupleAttribute<Tuple,String> documentRevisionAttribute) {
		this.documentRevisionAttribute = documentRevisionAttribute;
	}
	
	@Parameter(
		name=DOCDATA_ATTR_PARAM, optional=true,
		description="This parameter specifies the attribute name in the output tuple that gets the documents JSON content. It must be of type 'rstring'. If this parameter is not set, an attribute named 'document' is expected."
	)
	public void setDocumentDataAttribute(String documentDataAttribute) {
		this.documentDataAttribute = documentDataAttribute;
	}
	
	@ContextCheck(compile = true)
    public static void compiletimeChecker(OperatorContextChecker checker) {
		StreamsHelper.validateInputAttribute(checker, DOCID_ATTR_PARAM, DOCID_ATTR_DEFAULT, MetaType.RSTRING);
		StreamsHelper.validateOutputAttribute(checker, DOCDATA_ATTR_PARAM, DOCDATA_ATTR_DEFAULT, MetaType.RSTRING);
	}	
	
	@ContextCheck(compile = false, runtime = true)
    public static void runtimeChecker(OperatorContextChecker checker) {
		StreamsHelper.validateOutputAttributeRuntime(checker, DOCDATA_ATTR_PARAM, DOCDATA_ATTR_DEFAULT, MetaType.RSTRING);
	}
	
	// documentation ------------------------------------------------------------------------------------------
	
	// TODO improve
	public static final String operatorDescription =
		"This operator fetches documents from the configured Cloudant database and sends them as JSON strings to downstream operators."
		+ "The id and the revision of the document to retrieve can be specified.";

	// TODO improve 
	public static final String iport0Description =
		"The schema for this port must contain an attributes for the document id (field _id in Cloudant). It can contain an attribute with the revision (field _rev in Cloudant).";

	// TODO improve
	public static final String oport0Description =
		"The output port contains an attribute of type rsting that holds the JSON document fetched from the database."
		+ "Optionally it can contain an attribute for error messages."
		+ "Matching attributes are copied from the input port to the output port.";

}

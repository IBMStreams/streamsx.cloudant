//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant.client;

import org.apache.log4j.Logger;

import com.cloudant.client.api.Database;

/**
 * This is the interface between the Streams operators and the Cloudant 
 * client implementation. All code in the client namespace is supposed
 * to not have any dependencies or knowledge about Streams
 */
public interface Client {

	/**
	 * The client shall log valuable information for debugging to this logger
	 */
	void setLogger(Logger logger);
	
	/**
	 * Store the configuration in the client 
	 * @param config client config 
	 */
	void setConfiguration(Configuration config);
	
	/**
	 * check if the current configuration information is sufficient to perform operations 
	 * against the database
	 * @return true if the configuration is ok, false otherwise
	 */
	boolean validateConfiguration();
	
	/**
	 * Connect to the database. The method shall respect parameters from the configuration passed in.
	 */
	boolean connect();
	
	/**
	 * Disconnect fronm the database and clean up any resources left over
	 */
	void disconnect();
	
	/**
	 * Read a document from Cloudant. This method shall respect the read timeout parameter from the 
	 * configuration. The result indicates any errors. The method must not throw exceptions.
	 * @param id the id of the document to fetch
	 * @param revision the revision of the document. If set to null (or not present at all), the latest revision is returned
	 * @return The outcome of the operation. Contains error codes and the document as string
	 */
	FetchResult fetchDocument(String id, String revision);
	FetchResult fetchDocument(String id);
	
	/**
	 * Store a document in Cloudant. If a document with the same id is already in the database, the method
	 * tries to overwrite thhe existing document. The result indicates any errors. The method must not throw exceptions.
	 * @param document the Json String to store in the database. Can contain an _id field. If no _id field is contained, and the id paramter is null, Cloudant will assign a unique id.
	 * @param id the id of the document. If not null, the _id field in the document string (if any) is overwritten. 
	 * @param revision the revision to set in the document. If not null, the _rev field in the document string (if any) is overwritten. No retry is performed if the given ref leads to a document conflict
	 * @return The outcome of the operation
	 */
	StoreResult storeDocument(String document, String id, String revision);
	StoreResult storeDocument(String document, String id);
	StoreResult storeDocument(String document);

	/**
	 * Create a continous changes feed reader
	 * @param useAPI if true, the reader from the standard Java client is used. If false the reader directly based on the REST API is used 
	 * @param since the sequence value to start with
	 * @param getDocument if true get the whole document, else only the id is fetched
	 * @return
	 */
	ChangesReader getReader(boolean useAPI, String since, boolean getDocument);
	
}

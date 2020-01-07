//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.cloudant.client.api.Changes;
import com.cloudant.client.api.ClientBuilder;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.cloudant.client.api.model.ChangesResult;
import com.cloudant.client.api.model.Response;
import com.google.gson.JsonObject;

/**
 * This class provides a client implementation using the officially supported Cloudant Java API
 *
 */
public class ClientImpl implements Client, ChangesReader {

	// external properties
	private Logger logger = null;
	private Configuration cfg = null;
	
	// internal properties
	private boolean isConnected = false;
	private CloudantClient cloudantClient = null;
	private Database database = null;
	private boolean readerGetDocument = false;
	private String readerSince = "0";
	private Changes databaseChanges = null;
	
	public ClientImpl(Configuration config) {
		super();
		this.cfg = config;
	}

	@Override
	public void setConfiguration(Configuration config) {
		cfg = config;
		logger.trace("Configuration set: " + config.toString() );
	}

	@Override
	public boolean connect() {
		if (isConnected) {
			logger.warn("connecting to database requested, although the current connection is still alive");
			disconnect();
		}
		
		if (!validateConfiguration()) {
			logger.fatal("Configuration is invalid: " + cfg.toString());
			return false;
		}
		
		ClientBuilder cb = null;
		try {
			cb = ClientBuilder.url(new URL(cfg.getUrl()));
		} catch (MalformedURLException e) {
			logger.fatal("Malformed URL exception: " + cfg.getUrl());
			e.printStackTrace();
		}
		
		if (null == cb) {
			return false;
		}
		
		// set user
		cb.username(cfg.getUserName());
		
		// use either the password or the iamApiKey
		if (cfg.getIAMApiKey() == null) {
			cb.password(cfg.getPassword());
		} else {
			cb.iamApiKey(cfg.getIAMApiKey());
		}
		
		if (cfg.isDisableSSLVerification()) {
			cb.disableSSLAuthentication();
		}
		cb.connectTimeout(cfg.getConnectTimeout(), java.util.concurrent.TimeUnit.SECONDS);
		cb.readTimeout(cfg.getReadTimeout(), java.util.concurrent.TimeUnit.SECONDS);
		cb.maxConnections(cfg.getMaxConnections());
		cloudantClient = cb.build();
		
		database = cloudantClient.database(cfg.getDatabase(), false);
		logger.trace("successfully connected to database, config: " + cfg.toString());

		return true;
	}

	@Override
	public void disconnect() {
		if (null != cloudantClient) {
			cloudantClient.shutdown();
			cloudantClient = null;
		}
		database = null;
		logger.trace("Disconnected client");
	}

	/*
	 * Perform document save or update on the database
	 * if update is false, the doc must not contain a _rev attribute
	 * if update flag is true, the document must contain a _rev attrribute 
	 */
	private StoreResult saveOrUpdate(JsonObject docObject, boolean update) {
		StoreResult result = new StoreResult();
		String error = null;
		
		Response response = null;
		try {
			if (update) {
				response = database.update(docObject);
			} else {
				response = database.save(docObject);
			}
		} catch (Exception e) {
			error = "Exception during save: " + e.getMessage();
			result.setErrorMessage(error);
			if (e.getClass().getName().equals("com.cloudant.client.org.lightcouch.DocumentConflictException")) {
				result.setStatus(StoreResult.Status.DOCCONFLICT);
				logger.trace("Document conflict during save/update, giving up");
			} else {
				result.setStatus(StoreResult.Status.OTHER);
				logger.error(error);
			}
		}

		// exception occured
		if (null != error) return result;
		
		// check response if errors occured
		if (response != null && response.getError() != null && !response.getError().equals("")) {
			result.setStatus(StoreResult.Status.OTHER);
			result.setErrorMessage(response.getError());
		} else {
			result.setStatus(StoreResult.Status.OK);
		}

		// TODO remove this after testing
		if (null != response) {
			logger.trace("ERR : " + response.getError());
			logger.trace("REAS: " + response.getReason());
			logger.trace("STAT: " + response.getStatusCode());
		}
		
		return result;
	}
	
	@Override
	public StoreResult storeDocument(String document, String id, String revision) {
		StoreResult result = new StoreResult();
		
		// convert to object and set id and rev if needed
		JsonString doc = new JsonString(document);
		if (null != id && !id.equals("")) {
			doc.setTopLevelStringProperty("_id", id);
		}
		if (null != revision && !revision.equals("")) {
			doc.setTopLevelStringProperty("_rev", revision);
		}
		// JsonObject docObject = doc.getJsonObject();

		// check if id and/or rev are present. Combinations are handled as follows :
		// id present / rev present
		// 1) true/true		ok, call method update() to overwrite the existing doc, bail out if we get a document conflict
		// 2) true/false	ok, call method save(), if a document with this id already exists we get an error.
		//    In this case we read the doc get the current rev and call update() to overwrite the doc
		// 3) false/true	this is an error, without id Cloudant create a new doc and assignes id and rev on its own
		// 4) false/false	ok, call method save(), Cloudant assigns id and rev on its own 
		boolean hasId = (doc.getTopLevelStringProperty("_id") == null) ? false : true;
		boolean hasRev = (doc.getTopLevelStringProperty("_rev") == null) ? false : true;
		
		// case 3
		if (!hasId && hasRev) {
			result.setStatus(StoreResult.Status.BADINPUT);
			result.setErrorMessage("The document does not have an _id attribute, but has a _rev attribute. This combination is not allowed.");
			return result;
		}
		
		// case 1, update
		if (hasId && hasRev) {
			return saveOrUpdate(doc.getJsonObject(), true);
		}
		
		// case 4, save
		if (!hasId && !hasRev) {
			return saveOrUpdate(doc.getJsonObject(), false);
		}

		// case 2, save and update if fails
		if (hasId && !hasRev) {
			
			// first try to save
			JsonObject docObject = doc.getJsonObject();
			StoreResult saveRes = saveOrUpdate(docObject, false);
			if (saveRes.getStatus() != StoreResult.Status.DOCCONFLICT) {
				// give up if another error occured
				return saveRes;
			} else {
				// try to fix the revision 
				FetchResult fetchRes = fetchDocument(doc.getTopLevelStringProperty("_id"));
				if (!fetchRes.isValid()) {
					result.setStatus(StoreResult.Status.CANTREAD);
					result.setErrorMessage("Error during get revision: " + fetchRes.getErrorMessage());					
					return result;
					
				}
				JsonString fetched = new JsonString(fetchRes.getResult());
				String latestRev = fetched.getTopLevelStringProperty("_rev");
				logger.trace("update doc, fetched revision: " + latestRev);
				docObject.addProperty("_rev", latestRev);
				return saveOrUpdate(docObject, true);
			}
		}
		
		// should never come here
		return result;
	}

	@Override
	public StoreResult storeDocument(String document, String id) {
		return storeDocument(document, id, null);
	}

	@Override
	public StoreResult storeDocument(String document) {
		return storeDocument(document, null, null);
	}
	
	@Override
	public FetchResult fetchDocument(String id, String revision) {
		FetchResult res = new FetchResult();
		java.io.InputStream inputStream = null;
		
		String error = null;
		// try to read the document
		try {		
			if (null == revision) {
				inputStream = database.find(id);
			} else {
				inputStream = database.find(id,revision);
			}
		} catch(Exception e) {
			error = "Exception during find: " + e.getMessage();
			res.setErrorMessage(error);
			if (e.getClass().getName().equals("com.cloudant.client.org.lightcouch.NoDocumentException")) {
				res.setStatus(FetchResult.Status.NOTFOUND);
				logger.trace("No Document found exception");
			} else {
				res.setStatus(FetchResult.Status.OTHER);
				logger.error(error);
			}

		}
		
		// parse the result
		if (null == inputStream) {
			res.setErrorMessage("Unspecified error during fetching document with id: " + id);
		} else {
			String result = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
			try {
				inputStream.close();
			} catch (IOException e) {
				logger.trace("error closing input stream: " + e.getMessage());
			}
			res.setResult(result);
			res.setStatus(FetchResult.Status.OK);
		}
		return res;
	}

	@Override
	public FetchResult fetchDocument(String id) {
		return fetchDocument(id, null);
	}

	@Override
	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	@Override
	public boolean validateConfiguration() {
		if (cfg == null) return false;
		if (cfg.getUrl() == null) return false;
		if (cfg.getUserName() == null) return false;
		if ((cfg.getPassword() == null) && (cfg.getIAMApiKey() == null)) return false;
		if (cfg.getDatabase() == null) return false;
		return true;
	}

	public Database getDatabase() {
		return database;
	}

	@Override
	public ChangesReader getReader(boolean useAPI, String since, boolean getDocument) {
		if (useAPI) {
			if (initReader(since, getDocument)) {
				return this;
			}
			return null;
		}
		DirectReader dr = new DirectReader(cfg);
		if (!dr.initReader(since, getDocument, cfg.getReaderHeartbeatInterval())) {
			return null;
		}
		
		
		
		return dr;
	}

	private boolean initReader(String since, boolean getDocument) {
		readerGetDocument = getDocument;
		readerSince = since;
		
    	if (since.equals("0")) {
    		databaseChanges = database.changes().includeDocs(getDocument).heartBeat(cfg.getReaderHeartbeatInterval()).continuousChanges();
    	} else {
    		databaseChanges = database.changes().since(readerSince).includeDocs(getDocument).heartBeat(cfg.getReaderHeartbeatInterval()).continuousChanges();
    	}
    	
    	if (databaseChanges == null) {
    		return false;
    	}
    	return true;
	}

	@Override
	public ReaderResult getNext() {
		ReaderResult res = new ReaderResult();
		
		if (databaseChanges.hasNext()) {
			ChangesResult.Row feed = databaseChanges.next();
			res.setId(feed.getId());
			res.setSeq(feed.getSeq());
			res.setDeleted(feed.isDeleted());
			java.util.List<ChangesResult.Row.Rev> revs = feed.getChanges();

			if (null != revs && revs.size() > 0) {
				String rev = revs.get(0).getRev();
				res.setRev(rev);
			}

			if (readerGetDocument) {
				JsonObject obj = feed.getDoc();
				String doc = obj.toString();
				res.setDoc(doc);
			}
			res.setStatus(ReaderResult.Status.OK);
		} else {
			res.setStatus(ReaderResult.Status.HEARTBEAT);
		}
		return res;
	}

	@Override
	public void stopReading() {
        // stop the feed if active
        if (null != databaseChanges) {
        	databaseChanges.stop();
        	databaseChanges = null;
        }
	}
	
}

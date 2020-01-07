//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant.util;

import com.ibm.streamsx.cloudant.client.Client;
import com.ibm.streamsx.cloudant.client.FetchResult;
import com.ibm.streamsx.cloudant.client.JsonString;
import com.ibm.streamsx.cloudant.client.StoreResult;

/**
 * This implementation of the interface uses the CLoudant database itself 
 * to store the sequence value.
 */
public class DefaultSequenceManager implements SequenceManager {

	// the cloudant client
	private Client client = null;

	// error string
	private String lastError = null;
	
	// the _id of the docuemnt holding the seq value
	private String seqEntryId = null;
	
	// the name of the sequence value property in the JSON document
	private final static String SEQVALUE_PROPERTY = "sequenceValue";

	// flag to remember if the entry exists and was read already
	private boolean seqEntryExists = false;

	// the cached sequence value 
	private String seqValue = null;
	
	/**
	 * construct the manager.
	 * The client must point to a com.ibm.streams.cloudant.client.Client instance, that is already connected.
	 * @param client the client instance 
	 * @param seqEntryId the _id of the document that is used to track the sequence value
	 */
	public DefaultSequenceManager(Client client, String seqEntryId) {
		super();
		this.client = client;
		this.seqEntryId = seqEntryId;
	}

	@Override
	public boolean initialize() {
		setLastError("");
		if (null == seqEntryId) {
			setLastError("initialize(): storageId is null");
			return false;
		}
		if (null == client) {
			setLastError("initialize(): client object is null");
			return false;
		}
		return true;
	}
	
	// read the entry from Cloudant only if we never read it before. Otherwise the cached value can be used
	private boolean readEntry() {
		if (seqEntryExists) {
			return true;
		}
		FetchResult res = client.fetchDocument(seqEntryId); 
		if (res.isValid()) {
			JsonString entry = new JsonString(res.getResult());
			seqValue = entry.getTopLevelStringProperty(SEQVALUE_PROPERTY);
			seqEntryExists = true;
			return true;
		} else {
			if (res.getStatus() != FetchResult.Status.NOTFOUND) {
				setLastError("read(): " + res.getErrorMessage());
			}
		}
		return false;
	}

	@Override
	public String readOrCreate(String initialValue) {
		setLastError("");
		if (readEntry()) {
			return seqValue;
		}
		
		if (!updateOrCreate(initialValue)) {
			return null;
		}

		return seqValue;
	}

	@Override
	public boolean updateOrCreate(String value) {
		setLastError("");
		
		// prepare new document
		JsonString doc = new JsonString();
		doc.setTopLevelStringProperty(SEQVALUE_PROPERTY, value);
		StoreResult res = client.storeDocument(doc.getString(), seqEntryId);
		
		if (!res.isSuccess()) {
			setLastError("updateOrCreate(): " + res.getErrorMessage());
			return false;
		}
		
		seqValue = value;
		return true;
	}

	@Override
	public String read() {
		setLastError("");
		if (!readEntry()) {
			// the document was just not found 
			if (lastError.equals("")) {
				setLastError("read(): document not found");
			}
			return null;
		}
		return seqValue;
	}

	@Override
	public String getLastError() {
		return lastError;
	}

	private void setLastError(String lastError) {
		this.lastError = "DefaultSequenceManager error: " + lastError;
	}

}

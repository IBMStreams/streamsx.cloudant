//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant.client;

/**
 * This class represents the result row from the changes reader 
 * It holds error indications as well as result information
 *
 */
public class ReaderResult {
	
	/**
	 * Operation result status
	 */
	public enum Status {
		OK,
		HEARTBEAT,
		ERROR,
		TIMEOUT,
		UNKNOWN
	}
	
	// outcome of the operation
	private Status status = Status.UNKNOWN;

	// the resulting values from the change
	private String id = null;
	private String doc = null;
	private String rev = null;
	private String seq = null;
	private boolean deleted = false;
	

	// the error message, might be null or empty
	private String ErrorMessage = "";

	public ReaderResult(Status status) {
		super();
		this.status = status;
	}

	public ReaderResult() {
	}

	/**
	 * Check if the operation succeeded
	 * @return true if there was no failure, false otherwise
	 */
	public boolean isValid() {
		if (Status.OK == status || Status.HEARTBEAT == status) {
			return true;
		}
		return false;
	}
	
	public Status getStatus() {
		return status;
	}

	public String getErrorMessage() {
		return ErrorMessage;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public void setErrorMessage(String errorMessage) {
		ErrorMessage = errorMessage;
	}

	public boolean hasChange() {
		if (status == Status.OK) return true;
		else return false;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDoc() {
		return doc;
	}

	public void setDoc(String doc) {
		this.doc = doc;
	}

	public String getRev() {
		return rev;
	}

	public void setRev(String rev) {
		this.rev = rev;
	}

	public String getSeq() {
		return seq;
	}

	public void setSeq(String seq) {
		this.seq = seq;
	}

	public boolean isDeleted() {
		return deleted;
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}


	
}

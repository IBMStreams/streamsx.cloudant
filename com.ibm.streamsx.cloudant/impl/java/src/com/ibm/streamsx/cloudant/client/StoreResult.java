//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant.client;

/**
 * This class represents the result of a document store operation 
 * on the Cloudant database. It holds error indications and status
 *
 */
public class StoreResult {
	
	/**
	 * Operation result status
	 */
	public enum Status {
		OK,
		BADINPUT,
		DOCCONFLICT,
		CANTREAD,
		TIMEOUT,
		OTHER,
		UNKNOWN
	}
	
	// outcome of the operation
	private Status status = Status.UNKNOWN;

	// the error message, might be null or empty
	private String errorMessage = "";

	public StoreResult(Status status,String errorMessage) {
		super();
		this.status = status;
		this.errorMessage = errorMessage;
	}

	public StoreResult() {
	}

	/**
	 * Check if the operation succeeded
	 * @return true if there was no failure, false otherwise
	 */
	public boolean isSuccess() {
		if (Status.OK == status) {
			return true;
		}
		return false;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
	
}

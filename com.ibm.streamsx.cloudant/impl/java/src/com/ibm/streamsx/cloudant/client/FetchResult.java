//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant.client;

/**
 * This class represents the result of a document fetch operation 
 * on the Cloudant database. It holds error indications as well as result information
 *
 */
public class FetchResult {
	
	/**
	 * Operation result status
	 */
	public enum Status {
		OK,
		NOTFOUND,
		TIMEOUT,
		OTHER,
		UNKNOWN
	}
	
	// outcome of the operation
	private Status status = Status.UNKNOWN;

	// the resulting string
	private String result = null;

	// the error message, might be null or empty
	private String ErrorMessage = "";

	public FetchResult(Status status, String result, String errorMessage) {
		super();
		this.status = status;
		this.result = result;
		ErrorMessage = errorMessage;
	}

	public FetchResult() {
	}

	/**
	 * Check if the operation succeeded
	 * @return true if there was no failure, false otherwise
	 */
	public boolean isValid() {
		if (Status.OK == status) {
			return true;
		}
		return false;
	}
	
	public Status getStatus() {
		return status;
	}

	public String getResult() {
		return result;
	}

	public String getErrorMessage() {
		return ErrorMessage;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public void setResult(String result) {
		this.result = result;
	}

	public void setErrorMessage(String errorMessage) {
		ErrorMessage = errorMessage;
	}

	
}

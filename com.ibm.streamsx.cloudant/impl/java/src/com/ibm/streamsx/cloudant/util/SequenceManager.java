//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant.util;

/**
 * The SequenceManager is responsible for storing and retrieving configuration data
 * from an external database. It needs to maintain the last sequence value in an external
 * storage, so the application can pickup changes after a planned application restart.
 *
 */
public interface SequenceManager
{
	/**
	 * Any work that needs to be done at startup. Must be called before any other operation is called
	 * @return the outcome of the operation, true if succeeded, false otherwise
	 */
	boolean initialize();
	
	/**
	 * If the value is present in the external storage read and return it.
	 * if it is not present, create the entry and set it to the inital value
	 * @param initialValue the initial value. Used only if the entry is not already present. 
	 * @return the value read from the external storage. null if the operation failed
	 */
	String readOrCreate(String initialValue);
	
	/**
	 * Read the current sequence value
	 * @return the value. null if the operation failed
	 */
	String read();

	/**
	 * Set the value in the external storage to the specified string.
	 * If the storage entry for the sequence does not exist, it is created 
	 * @param value the value to set the sequence number to
	 * @return true if the operation succeeded, false otherwise
	 */
	boolean updateOrCreate(String value);
	
	/**
	 * Return the latest error occured 
	 * @return the error message. null if no errors happend so far
	 */
	String getLastError();
	
}

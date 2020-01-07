//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant.client;

public interface ChangesReader {

	ReaderResult getNext();
	
	void stopReading();
	
}

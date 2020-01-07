//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant.client;

/**
 * 5xx error from HTTP call; usually not permanent.
 */
public class ServerException extends Exception {

    public final int status;

    public ServerException(int status) {
        this.status = status;
    }
}

//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant.client;

/**
 * 4xx error from HTTP call; usually permanent (e.g., bad credentials).
 */
public class UnexpectedResponseException extends Exception {

    public final int status;

    public UnexpectedResponseException(int status) {
        this.status = status;
    }
}

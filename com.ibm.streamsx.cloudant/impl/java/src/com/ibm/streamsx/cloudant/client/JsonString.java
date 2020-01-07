//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant.client;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Implements helper methods to extract and set properties on a String containing JSON data,
 * and convert back and forth between string and JsonObject 
 * Uuses Gson to perform conversions to/from string representation
 */
public class JsonString {

	private String str = null;
	
	private JsonObject data = null;
	
	private boolean objectModified = false;

	public JsonString(String str) {
		super();
		this.str = str;
	}

	public JsonString() {
		super();
		this.str = "{ }";
	}
	
	public JsonObject getJsonObject() {
		return convertToObject();
	}

	public String getString() {
		if (objectModified) {
			str = data.toString();
			objectModified = false;
		}
		return str;
	}
	
	private JsonObject convertToObject() {
		if (data == null) {
			 data = new Gson().fromJson(str, JsonObject.class);
		}
		return data;
	}
	
	public String getTopLevelStringProperty(String name) {
		convertToObject();
		JsonElement e = data.get(name);
		if (null != e) return e.getAsString();
		return null;
	}

	public void setTopLevelStringProperty(String name, String value) {
		convertToObject();
		data.addProperty(name, value);
		objectModified = true;
	}
	
}

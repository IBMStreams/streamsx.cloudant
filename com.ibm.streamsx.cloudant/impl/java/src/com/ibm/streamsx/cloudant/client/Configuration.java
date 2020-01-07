//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant.client;

/**
 * Data structure to hold Cloudant client configuration data
 */
public class Configuration
{
	private String url = null;
	private String userName = null;
	private String password = null;
	private String database = null;
	private String vcapService = null;
	private boolean disableSSLVerification = false;
	private Long readTimeout = (long) 300;
	private Long connectTimeout = (long) 300;
	private int readerHeartbeatInterval = 2000; // in milliseconds 
	private int maxConnections = 6;
	private String IAMApiKey = null;
	
	public static Configuration getDefaultConfiguration() {
		return new Configuration();
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getVcapService() {
		return vcapService;
	}

	public void setVcapService(String vcapService) {
		this.vcapService = vcapService;
	}

	public boolean isDisableSSLVerification() {
		return disableSSLVerification;
	}

	public void setDisableSSLVerification(boolean disableSSLVerification) {
		this.disableSSLVerification = disableSSLVerification;
	}

	public Long getReadTimeout() {
		return readTimeout;
	}

	public void setReadTimeout(Long readTimeout) {
		this.readTimeout = readTimeout;
	}

	public Long getConnectTimeout() {
		return connectTimeout;
	}

	public void setConnectTimeout(Long connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public int getMaxConnections() {
		return maxConnections;
	}

	public void setMaxConnections(int maxConnections) {
		this.maxConnections = maxConnections;
	}

	public int getReaderHeartbeatInterval() {
		return readerHeartbeatInterval;
	}

	public void setReaderHeartbeatInterval(int readerHeartbeatInterval) {
		this.readerHeartbeatInterval = readerHeartbeatInterval;
	}

	public String getIAMApiKey() {
		return IAMApiKey;
	}

	public void setIAMApiKey(String IAMApiKey) {
		this.IAMApiKey = IAMApiKey;
	}

	@Override
	public String toString() {
		return "Configuration [url=" + url + ", userName=" + userName + ", password=" + password + ", database="
				+ database + ", vcapService=" + vcapService + ", disableSSLVerification=" + disableSSLVerification
				+ ", readTimeout=" + readTimeout + ", connectTimeout=" + connectTimeout + ", readerHeartbeatInterval="
				+ readerHeartbeatInterval + ", maxConnections=" + maxConnections + ", IAMApiKey=" + IAMApiKey + "]";
	}
	
}

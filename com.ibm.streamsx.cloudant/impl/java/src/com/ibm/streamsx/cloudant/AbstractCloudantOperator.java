//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.cloudant.client.Configuration;
import com.ibm.streamsx.cloudant.util.StreamsHelper;

/**
 * This class should be used as parent for the Cloudant operators
 * it holds common operator parameters and helper/default methods
 * @author brandtol
 */
@Libraries("opt/downloaded/*")
public class AbstractCloudantOperator extends AbstractOperator
{
	// parameter related members ----------------------------------------------------------------------
	
	// the base url to the Cloudant server
	private String url = null;
	
	// The username for connection
	private String username = null;
	
	// The password for the username
	private String password = null;
	
	// The IAM apikey for the user, if this is set thepassword is ignored
	// only works for StoreDocument and FetchDocument operators so far
	private String IAMApiKey = null;
	
	// The name of the database
	private String databaseName = null;
	
	// name of the output attribute to hold the error message
	private static final String ERRCODE_ATTR_PARAM = "errorCodeAttribute";
	private String errorCodeAttribute = null;
	
	// shall we preserve punctuations
    private boolean preservePunctuation = false;
	
	// The name of the applicaton config object
	private String appConfigName = null;
 
	// internal members ------------------------------------------------------------------------------
	
	// Logger for tracing.
    private static Logger logger = Logger.getLogger(AbstractCloudantOperator.class.getName());
	
    // data from application config object
    Map<String, String> appConfig = null;
    
	// operator methods ------------------------------------------------------------------------------

	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
	        super.initialize(context);
	        logger.trace("initialize AbstractCloudantOperator");
	        loadAppConfig(context);
	}
	
    // helper methods -----------------------------------------------------------------------------------
    
	/**
	 * cunstruct a new configuration object from the data known so far.
	 * It uses the operatore parameters where available. Or the entries from the application config, if available
	 * @return the Config used as input for the cloudant client
	 */
	protected Configuration getConfiguration() {
		Configuration cfg = Configuration.getDefaultConfiguration();
		
		if (null != url)  {
			cfg.setUrl(url);
		} else {
			cfg.setUrl(appConfig.get("url"));
		}
		
		if (null != username) {
			cfg.setUserName(username);
		} else {
			cfg.setUserName(appConfig.get("username"));
		}

		if (null != password) {
			cfg.setPassword(password);
		} else {
			cfg.setPassword(appConfig.get("password"));
		}

		if (null != IAMApiKey) {
			cfg.setIAMApiKey(IAMApiKey);
		} else {
			cfg.setIAMApiKey(appConfig.get("IAMApiKey"));
		}

		if (null != databaseName) {
			cfg.setDatabase(databaseName);
		} else {
			cfg.setDatabase(appConfig.get("databaseName"));
		}

		return cfg;
	}
	
	/**
	 * set the error string to the tuple attribute configured to hold it
	 * @param tuple the output tuple to use
	 * @param error the error string to assign
	 */
	protected void setErrorCode(OutputTuple tuple, String error) {
		if (null != errorCodeAttribute) {
			tuple.setString(errorCodeAttribute, error);
		}
	}
	
	/**
	 * read the application config into a map
	 * @param context the operator context 
	 */
    protected void loadAppConfig(OperatorContext context) {
    	
    	// if no appconfig name is specified, create empty map
        if (appConfigName == null) {
        	appConfig = new HashMap<String,String>();
        	return;
        }

        appConfig = context.getPE().getApplicationConfiguration(appConfigName);
        if (appConfig.isEmpty()) {
            logger.warn("APPLICATION_CONFIG_NOT_FOUND_OR_EMPTY: " + appConfigName);
        }
        
        for (Map.Entry<String, String> kv : appConfig.entrySet()) {
        	logger.trace("Found application config entry : " + kv.getKey() + "=" + kv.getValue());
        }
        
    }
    
    // Connection related parameters -------------------------------------------------------------- 
    
    @Parameter(name="url", description="The base URL of the CLoudant REST API endpoint.", optional=true)
    public void setUrl(String url) {
    	this.url = url;
    }

    @Parameter(name="username", description="The username used to authenticate against the database.", optional=true)
    public void setUsername(String userName) {
    	this.username = userName;
    }

    @Parameter(name="password", description="The password for the username.", optional=true)
    public void setPassword(String password) {
    	this.password = password;
    }

    @Parameter(name="IAMApiKey", description="The IAM API key for the user. If this parameter is specified, the password parameter is ignored.", optional=true)
    public void setIAMAiKey(String IAMApiKey) {
    	this.IAMApiKey = IAMApiKey;
    }
    
    @Parameter(name="databaseName", description="The name of the database", optional=true)
    public void setDatabaseName(String dbname) {
    	this.databaseName = dbname;
    }

    // other common parameters -----------------------------------------------------------------------
    
	@Parameter(
		name=ERRCODE_ATTR_PARAM, optional=true,
		description="This parameter specifies the name of an output attribute in the output tuple that will hold error messages returned by the Cloudant database opration."
	)
	public void setErrorCodeAttribute(String errorCodeAttribute) {
		this.errorCodeAttribute = errorCodeAttribute;
	}	

	@Parameter(
		name="preservePunctuation", optional=true,
		description="If set to true then the operator forwards punctuation from input port 0 to output port 0. The default value is false and a window punctuation is generated on output port 0 after object is read."
	)
	public void setPreservePunctuation(boolean preservePunctuation) {
		this.preservePunctuation = preservePunctuation;
	}
	
	@Parameter(
		name="appConfigName", optional = true,
		description="Specifies the name of the application configuration that contains Cloudant connection related configuration parameters. The keys in the application configuration have the same name as the operator parameters."
		+ "The following keys are supported: url, username, password, databaseName."
		+ "If a value is specified in the application configuration and as operator parameter, the operator parameter value takes precedence."
	)
	public void setAppConfigName(String appConfigName) {
		this.appConfigName = appConfigName;
	}	
	
	@ContextCheck(compile = false, runtime = true)
    public static void runtimeChecker(OperatorContextChecker checker) {
		StreamsHelper.validateOutputAttributeRuntime(checker, ERRCODE_ATTR_PARAM, null, MetaType.RSTRING);
	}
    
    // parameter related getters ---------------------------------------------------------------------
    
	public String getUrl() {
		return url;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getErrorCodeAttribute() {
		return errorCodeAttribute;
	}

	public boolean isPreservePunctuation() {
		return preservePunctuation;
	}

	public String getAppConfigName() {
		return appConfigName;
	}

	public String getIAMApiKey() {
		return IAMApiKey;
	}

}

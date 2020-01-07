//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant.util;

import java.util.List;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;

/**
 * contains methods to validate compile and runtime checking
 *
 */
public class StreamsHelper {
	
	/**
	 * check that a suitable attribute is availabe in the operator input stream
	 * @param checker the context checker 
	 * @param attributeParameter the parameter name specifying the attribute name
	 * @param defaultAttribute the default attribute name, used if the parameter is not set
	 * @param attributeType the type of the attribute
	 * @param inputPort the number of the port
	 */
	public static void validateInputAttribute(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType, int inputPort) {
		OperatorContext ctx = checker.getOperatorContext();

		List<StreamingInput<Tuple>> inputPorts = ctx.getStreamingInputs();
		if (inputPort >= inputPorts.size()) {
			checker.setInvalidContext("INPUT_PORT_NOT_CONFIGURED: " + inputPort, new Object[0]);
			return;
		}
		
		// Streams handles all type and name checking on its own if the paramter is specified
		if(!ctx.getParameterNames().contains(attributeParameter)) {
			StreamSchema schema = ctx.getStreamingInputs().get(inputPort).getStreamSchema();
			Attribute attr = schema.getAttribute(defaultAttribute);	
			
			// check existence of attribute 
			if (attr == null) {
				checker.setInvalidContext("DEFAULT_ATTRIBUTE_NOT_FOUND: " + defaultAttribute, new Object[0]);
				return;
			}
			
			// check correct type of attribute
			if(!checker.checkAttributeType(attr, attributeType)) {
				checker.setInvalidContext("SPECIFIED_ATTRIBUTE_WRONG_TYPE, expected: " + attributeType.toString(), new Object[0]);
				return;
			}
		}
	}
	
	public static void validateInputAttribute(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType) {
		validateInputAttribute(checker, attributeParameter, defaultAttribute, attributeType, 0);
	}

	/**
	 * check that a suitable attribute is availabe in the operator output stream
	 * @param checker 
	 * @param attributeParameter
	 * @param defaultAttribute
	 * @param attributeType
	 * @param outputPort
	 */
	public static void validateOutputAttribute(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType, int outputPort) {
		OperatorContext ctx = checker.getOperatorContext();
		
		// cannot validate parameter values at compile time, only check for default attribute here
		if(!ctx.getParameterNames().contains(attributeParameter) && defaultAttribute != null) {

			// check that we have the port
			List<StreamingOutput<OutputTuple>> outputPorts = ctx.getStreamingOutputs();
			if (outputPort >= outputPorts.size()) {
				checker.setInvalidContext("OUTPUT_PORT_NOT_CONFIGURED: " + outputPort, new Object[0]);
				return;
			}
			
			StreamSchema schema = ctx.getStreamingOutputs().get(outputPort).getStreamSchema();
			Attribute attr = schema.getAttribute(defaultAttribute);
			
			// check existence of attribute 
			if (attr == null) {
				checker.setInvalidContext("DEFAULT_ATTRIBUTE_NOT_FOUND: " + defaultAttribute, new Object[0]);
				return;
			}
			
			// check correct type of attribute
			if(!checker.checkAttributeType(attr, attributeType)) {
				checker.setInvalidContext("SPECIFIED_ATTRIBUTE_WRONG_TYPE, expected: " + attributeType.toString(), new Object[0]);
				return;
			}
		}
	}

	public static void validateOutputAttribute(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType) {
		validateOutputAttribute(checker, attributeParameter, defaultAttribute, attributeType, 0);
	}
	
	/**
	 * check that a parameter specified output attribute is availabe in the operator output stream at runtime, with correct type 
	 * @param checker
	 * @param attributeParameter
	 * @param defaultAttribute
	 * @param attributeType
	 * @param outputPort
	 */
	public static void validateOutputAttributeRuntime(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType, int outputPort) {
		OperatorContext ctx = checker.getOperatorContext();

		if(ctx.getParameterNames().contains(attributeParameter)) {
			
			// check that we have an output port at all
			List<StreamingOutput<OutputTuple>> outputPorts = ctx.getStreamingOutputs();
			if (outputPort >= outputPorts.size()) {
				checker.setInvalidContext("OUTPUT_PORT_NOT_CONFIGURED: " + outputPort, new Object[0]);
				return;
			}
			
			Attribute attr = null;
            String attrName = ctx.getParameterValues(attributeParameter).get(0);
            StreamSchema schema = ctx.getStreamingOutputs().get(outputPort).getStreamSchema();
            attr = schema.getAttribute(attrName);

            // check existence of attribute
            if(attr == null) {
            	checker.setInvalidContext("SPECIFIED_OUTPUT_ATTRIBUTE_NOT_FOUND",new Object[0]);
            	return;
		    }
            
			// check correct type of attribute
			if(!checker.checkAttributeType(attr, attributeType)) {
				checker.setInvalidContext("SPECIFIED_ATTRIBUTE_WRONG_TYPE, expected: " + attributeType.toString(), new Object[0]);
				return;
			}
		}
	}

	public static void validateOutputAttributeRuntime(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType) {
		validateOutputAttributeRuntime(checker, attributeParameter, defaultAttribute, attributeType, 0);
	}

}

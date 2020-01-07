//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.cloudant.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class DirectReader implements ChangesReader {

    private final static Logger logger = Logger.getLogger(DirectReader.class.getName());
    
    private String since;
    private boolean getDocument;
    private int heartbeat;

    private Configuration cfg = null;

    private BufferedReader breader = null;
    
    private static final Gson gson = new Gson();

	public DirectReader(Configuration cfg) {
		super();
		this.cfg = cfg;
	}
	
	public boolean initReader(String since, boolean getDocument, int heartbeat) {
		this.since = since;
		this.getDocument = getDocument;
		this.heartbeat = heartbeat;
		
		boolean success = false;
		try {
			breader = createReaderFromSeq();
			success = true;
		} catch (IOException | BadRequestException | ServerException | UnexpectedResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return success;
	}

	@Override
	public ReaderResult getNext() {
		ReaderResult res = new ReaderResult();
        String line = null;

        // Read the next line (empty = heartbeat, ignore; null = end of stream)
        try {
			line = breader.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        if (line != null && line.isEmpty()) {
        	res.setStatus(ReaderResult.Status.HEARTBEAT);
        	return res;
        }

        res.setStatus(ReaderResult.Status.OK);
        
        if (line.endsWith(",")) {
        	line = line.substring(0, line.length() - 1);
        }
        
        ChangesRow r = gson.fromJson(line, ChangesRow.class);
        //System.out.println("line: " + line);        
        res.setId(r.getId());
        //System.out.println("idok");        
        res.setDeleted(r.deleted);
        //System.out.println("delok");
        res.setSeq(r.getSeq());
        //System.out.println("seqok");
        
        if (getDocument) {
			JsonObject obj = r.getDoc();
			String doc = obj.toString();
			res.setDoc(doc);
		}

		java.util.List<ChangesRow.Rev> revs = r.getChanges();
		if (null != revs && revs.size() > 0) {
			String rev = revs.get(0).getRev();
			res.setRev(rev);
		}
		
		return res;
	}

	@Override
	public void stopReading() {
		// TODO Auto-generated method stub

	}

	private BufferedReader createReaderFromSeq() throws IOException, BadRequestException, ServerException, UnexpectedResponseException {

		String url = UrlBuilder.changes(
        		cfg.getUrl(),
        		cfg.getDatabase(),
        		"continuous",
        		this.since,
        		0,
        		0,
        		this.getDocument,
        		this.heartbeat,
        		1
        );

		logger.trace("Starting changes watcher for URL: " + url);

        Request.Builder builder = new Request.Builder().url(url);
        String credential = Credentials.basic(cfg.getUserName(), cfg.getPassword());
        builder.header("Authorization", credential);
        Request request = builder.build();
        
        OkHttpClient client = new OkHttpClient().newBuilder().
        		connectTimeout(cfg.getConnectTimeout(),TimeUnit.SECONDS).
        		readTimeout(cfg.getReadTimeout(),TimeUnit.SECONDS).build();
        		
        Response response = client.newCall(request).execute();

        int status_code = response.code();

        if (status_code == 200) {
            InputStream in = response.body().byteStream();
            return new BufferedReader(new InputStreamReader(in));
        } else if (status_code > 400 && status_code < 500) {
            response.close();
            throw new BadRequestException(status_code);
        } else if (status_code > 500) {
            response.close();
            throw new ServerException(status_code);
        } else {
            // Basically 1xx, 2xx not 200 and 3xx are unexpected for this call.
            response.close();
            throw new UnexpectedResponseException(status_code);
        }
    }

}

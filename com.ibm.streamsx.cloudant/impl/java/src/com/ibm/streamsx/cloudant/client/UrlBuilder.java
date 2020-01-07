/**
 * Copyright (c) 2015 IBM Cloudant. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.ibm.streamsx.cloudant.client;

/**
 * Created by mike on 21/08/2016.
 */
public class UrlBuilder {

    public static String databaseUrl(String instanceUrl, String databaseName) {
        return String.format("%s%s", ensureEndsWith(instanceUrl, "/"), databaseName);
    }

    public static String changes(String instanceUrl, String databaseName, String feed, String seq,
                           long timeout, long limit, boolean includeChanges, long heartbeat, long seqInterval) {

        String databaseUrl = databaseUrl(instanceUrl, databaseName);

        String url = String.format("%s/_changes?feed=%s&since=%s",
                databaseUrl, feed, seq);
        if (includeChanges) {
            url = String.format("%s&include_docs=true", url);
        }
        if (timeout > 0) {
            url = String.format("%s&timeout=%d", url, timeout);
        }
        if (limit > 0) {
            url = String.format("%s&limit=%d", url, limit);
        }
        if (heartbeat > 0) {
            url = String.format("%s&heartbeat=%d", url, heartbeat);
        }
        if (seqInterval > 0) {
            url = String.format("%s&seq_interval=%d", url, seqInterval);
        }

        return url;
    }

    private static String ensureEndsWith(String base, String suffix) {
        if (base.endsWith(suffix)) {
            return base;
        } else {
            return String.format("%s%s", base, suffix);
        }
    }
}

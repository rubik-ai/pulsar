/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.functions.Utils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Utils {
    private static final String DEPOT_SERVICE_URL = "DEPOT_SERVICE_URL";
    private static final String DATAOS_RUN_AS_APIKEY = "DATAOS_RUN_AS_APIKEY";
    private static final String DATAOS_RUN_AS_USER_AGENT = "DATAOS_RUN_AS_USER_AGENT";
    private static final String PULSAR_SERVICE_URL = "PULSAR_SERVICE_URL";

    public static String getDepotServiceUrl() {
        String depotServiceUrl = System.getenv(DEPOT_SERVICE_URL);
        return depotServiceUrl != null ? depotServiceUrl.replaceAll("/+$", "") : "http://localhost:8000";
    }

    public static String getPulsarServiceUrl() {
        String depotServiceUrl = System.getenv(PULSAR_SERVICE_URL);
        return depotServiceUrl != null ? depotServiceUrl.replaceAll("/+$", "") : "http://localhost:8000";
    }
    public static String getUserAgent() {
        String userAgent = System.getenv(DATAOS_RUN_AS_USER_AGENT);
        return userAgent != null ? userAgent : "pulsar-io-lakehouse";
    }

    public static String getUserApiKey() {
        String apiKey = System.getenv(DATAOS_RUN_AS_APIKEY);
        if (apiKey != null) {
            return apiKey;
        } else {
            throw new RuntimeException("Fatal! Environment variable " + DATAOS_RUN_AS_APIKEY + " not provided.");
        }
    }
}
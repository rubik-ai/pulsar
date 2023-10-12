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
package org.apache.pulsar.functions.instance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.dataos.ds.client.DepotServiceClient;
import io.dataos.ds.spark.SparkKeyValuePropsSecretLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.Utils.Utils;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class PulsarFunctionInit {

    private static final String YAML_FILE_PATH = "/etc/dataos/config/jobconfig.yaml";
    private static final int MAX_BUFFERED_TUPLES = 100;
    private static final int PORT = 9093;
    private static final int METRICS_PORT = 9094;
    private static final int EXPECTED_HEALTH_CHECK_INTERVAL = -1;
    private static final String CLUSTER_NAME = "pulsar";
    private static final String CLIENT_AUTH_PLUGIN = "org.apache.pulsar.client.impl.auth.AuthenticationToken";
    private static final String JAR_PATH = "/pulsar/connectors/pulsar-io-lakehouse-2.11.0-dataos.2-cloud.nar";
    private static final int INSTANCE_ID = 1;
    private static final String FUNCTION_ID = "47fac49e-a975-44c1-9a76-2b1461489a51";
    private static final String FUNCTION_VERSION = "0";

    public static void main(String[] args) {
        if (args.length == 0) {
            throw new RuntimeException("Fatal! jobconfig.yaml path not provided as argument.");
        } else {
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            ObjectMapper jsonMapper = new ObjectMapper();

            try {
                JsonNode yamlNode = yamlMapper.readTree(new File(args[0]));
                convertYamlToJsonAndStartFunction(yamlNode, jsonMapper);
            } catch (Exception e) {
                log.error("Error reading YAML file: {}", e.getMessage());
            }
        }
    }

    private static void convertYamlToJsonAndStartFunction(JsonNode yamlNode, ObjectMapper jsonMapper) throws Exception {
        if (yamlNode.has("sink") && yamlNode.get("sink").isObject()) {
            ObjectNode sinkNode = (ObjectNode) yamlNode.get("sink");
            if (sinkNode.has("configs") && sinkNode.get("configs").isObject()) {
                ObjectNode configsNode = (ObjectNode) sinkNode.get("configs");
                if (configsNode.has("catalogName")) {
                    Map<String, String> secrets = loadSecrets(configsNode.get("catalogName").asText());
                    secrets.forEach(configsNode::put);
                }
                sinkNode.put("configs", sinkNode.get("configs").toString());
            }
        }
        String jsonString = jsonMapper.writeValueAsString(yamlNode);
        startPulsarFunction(jsonString);
    }

    private static void startPulsarFunction(String jsonString) throws Exception {
        String pulsarServiceUrl = Utils.getPulsarServiceUrl();
        String userApiKey = Utils.getUserApiKey();

        String[] pulsarFunctionArguments = {
                "--jar", JAR_PATH,
                "--instance_id", String.valueOf(INSTANCE_ID),
                "--function_id", FUNCTION_ID,
                "--function_version", FUNCTION_VERSION,
                "--pulsar_serviceurl", pulsarServiceUrl,
                "--max_buffered_tuples", String.valueOf(MAX_BUFFERED_TUPLES),
                "--port", String.valueOf(PORT),
                "--metrics_port", String.valueOf(METRICS_PORT),
                "--expected_healthcheck_interval", String.valueOf(EXPECTED_HEALTH_CHECK_INTERVAL),
                "--cluster_name", CLUSTER_NAME,
                "--use_tls", String.valueOf(false),
                "--function_details", String.format("'%s'",jsonString),
                "--client_auth_plugin", CLIENT_AUTH_PLUGIN,
                "--client_auth_params", userApiKey
        };

        log.info("Starting Pulsar Function with arguments: {}", Arrays.toString(pulsarFunctionArguments));
        JavaInstanceMain.main(pulsarFunctionArguments);
    }

    private static Map<String, String> loadSecrets(String catalogName) {
        SparkKeyValuePropsSecretLoader secretLoader = new SparkKeyValuePropsSecretLoader(Objects.requireNonNull(getDepotClient()));
        Map<String, String> secrets = secretLoader.load(catalogName, "rw");
        Map<String, String> modifiedSecrets = new HashMap<>();

        secrets.forEach((key, value) -> {
            String modifiedKey = key.replaceFirst("fs", "hadoop.fs");
            modifiedSecrets.put(modifiedKey, value);
        });
        return modifiedSecrets;
    }

    private static DepotServiceClient getDepotClient() {
        try {
            String apiKey = Utils.getUserApiKey();
            String depotServiceUrl = Utils.getDepotServiceUrl();
            String userAgent = Utils.getUserAgent();
            return new DepotServiceClient.Builder()
                    .apikey(apiKey)
                    .url(depotServiceUrl)
                    .userAgent(userAgent)
                    .build();
        } catch (Exception e) {
            log.error("Error creating DepotServiceClient: {}", e.getMessage());
            return null;
        }
    }
}

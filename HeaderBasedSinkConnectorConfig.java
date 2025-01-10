/*
 * Copyright 2019 SMB GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package solutions.Infy.kafka.connect.config;


import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class HeaderBasedSinkConnectorConfig extends AbstractConfig {

    // Define configuration keys
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String HEADER_ROUTING_TOPIC = "header.routing.topic";
    public static final String DLQ_TOPIC = "dlq.topic";

    // Define documentation for each configuration
    private static final String BOOTSTRAP_SERVERS_DOC = "Comma-separated list of Kafka bootstrap servers.";
    private static final String HEADER_ROUTING_TOPIC_DOC = "Name of the compacted topic used for header-based routing.";
    private static final String DLQ_TOPIC_DOC = "Name of the Dead Letter Queue (DLQ) topic.";

    public HeaderBasedSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public HeaderBasedSinkConnectorConfig(final Map<String, String> parsedConfig) {
        super(conf(), parsedConfig);
    }

    // Define the configuration schema
    public static ConfigDef conf() {
        return new ConfigDef()
                .define(BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, BOOTSTRAP_SERVERS_DOC)
                .define(HEADER_ROUTING_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, HEADER_ROUTING_TOPIC_DOC)
                .define(DLQ_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DLQ_TOPIC_DOC);
    }
}

    // Getter methods for configurations
    /*public String getBootstrapServers() {

        return this.getString(BOOTSTRAP_SERVERS);
    }

    public String getHeaderRoutingTopic() {
        return this.getString(HEADER_ROUTING_TOPIC);
    }

    public String getDlqTopic() {
        return this.getString(DLQ_TOPIC);
    }
}*/
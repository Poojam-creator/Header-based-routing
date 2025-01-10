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

package solutions.Infy.kafka.connect.sink;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import solutions.Infy.kafka.connect.config.HeaderBasedSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import solutions.Infy.kafka.connect.Version;
import java.util.ArrayList;
import java.util.List;

public class HeaderBasedSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(HeaderBasedSinkConnector.class);

    private Map<String, String> config;


    @Override
    public void start(Map<String, String> properties) {
        log.info("started Connector along with properties -",properties.toString());
        this.config = properties;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HeaderBasedSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Started Tasks - " ,maxTasks);
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        Map<String,String> taskProps =new HashMap<>();
        taskProps.putAll(config);
        for (int i = 0; i < maxTasks; i++) {
            log.info("started Task -", i,"with taskProps" ,taskProps);
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        // No cleanup logic required
    }

    @Override
    public ConfigDef config() {
        return HeaderBasedSinkConnectorConfig.conf();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
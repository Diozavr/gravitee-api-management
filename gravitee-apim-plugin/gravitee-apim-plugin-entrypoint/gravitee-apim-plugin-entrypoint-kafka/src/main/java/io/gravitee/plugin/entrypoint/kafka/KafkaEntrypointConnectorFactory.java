/*
 * Copyright © 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.plugin.entrypoint.kafka;

import io.gravitee.gateway.reactive.api.ApiType;
import io.gravitee.gateway.reactive.api.ConnectorMode;
import io.gravitee.gateway.reactive.api.ListenerType;
import io.gravitee.gateway.reactive.api.connector.entrypoint.async.EntrypointAsyncConnectorFactory;
import io.gravitee.gateway.reactive.api.context.DeploymentContext;
import io.gravitee.gateway.reactive.api.exception.PluginConfigurationException;
import io.gravitee.gateway.reactive.api.helper.PluginConfigurationHelper;
import io.gravitee.gateway.reactive.api.qos.Qos;
import io.gravitee.plugin.entrypoint.kafka.configuration.KafkaEntrypointConnectorConfiguration;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class KafkaEntrypointConnectorFactory implements EntrypointAsyncConnectorFactory<KafkaEntrypointConnector> {

    private final PluginConfigurationHelper configurationHelper;

    @Override
    public ApiType supportedApi() {
        return KafkaEntrypointConnector.SUPPORTED_API;
    }

    @Override
    public Set<ConnectorMode> supportedModes() {
        return KafkaEntrypointConnector.SUPPORTED_MODES;
    }

    @Override
    public Set<Qos> supportedQos() {
        return KafkaEntrypointConnector.SUPPORTED_QOS;
    }

    @Override
    public ListenerType supportedListenerType() {
        return KafkaEntrypointConnector.SUPPORTED_LISTENER_TYPE;
    }

    @Override
    public KafkaEntrypointConnector createConnector(DeploymentContext deploymentContext, Qos qos, String configuration) {
        try {
            KafkaEntrypointConnectorConfiguration config = configurationHelper.readConfiguration(
                KafkaEntrypointConnectorConfiguration.class,
                configuration
            );
            return new KafkaEntrypointConnector(config);
        } catch (PluginConfigurationException e) {
            log.error("Unable to load kafka entrypoint configuration", e);
            return null;
        }
    }
}

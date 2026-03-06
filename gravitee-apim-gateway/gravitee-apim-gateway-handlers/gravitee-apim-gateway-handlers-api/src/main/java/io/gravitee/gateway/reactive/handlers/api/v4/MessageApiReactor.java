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
package io.gravitee.gateway.reactive.handlers.api.v4;

import io.gravitee.gateway.env.RequestTimeoutConfiguration;
import io.gravitee.gateway.opentelemetry.TracingContext;
import io.gravitee.gateway.reactive.api.context.DeploymentContext;
import io.gravitee.gateway.reactive.core.v4.endpoint.EndpointManager;
import io.gravitee.node.api.Node;
import io.gravitee.node.api.configuration.Configuration;
import io.gravitee.plugin.entrypoint.EntrypointConnectorPluginManager;

/**
 * Dedicated reactor for message APIs.
 *
 * <p>
 * This implementation currently reuses the TCP reactive handling pipeline and opens a dedicated extension point for
 * message-specific flow/security processing in follow-up iterations.
 */
public class MessageApiReactor extends TcpApiReactor {

    public MessageApiReactor(
        final Api api,
        final Node node,
        final Configuration configuration,
        final DeploymentContext deploymentContext,
        final EntrypointConnectorPluginManager entrypointConnectorPluginManager,
        final EndpointManager endpointManager,
        final RequestTimeoutConfiguration requestTimeoutConfiguration,
        final TracingContext tracingContext
    ) {
        super(
            api,
            node,
            configuration,
            deploymentContext,
            entrypointConnectorPluginManager,
            endpointManager,
            requestTimeoutConfiguration,
            tracingContext
        );
    }

    @Override
    public String toString() {
        return "MessageApiReactor API id[" + api.getId() + "] name[" + api.getName() + "] version[" + api.getApiVersion() + ']';
    }
}

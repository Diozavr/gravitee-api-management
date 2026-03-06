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
import io.gravitee.gateway.reactive.api.ExecutionPhase;
import io.gravitee.gateway.reactive.api.context.DeploymentContext;
import io.gravitee.gateway.reactive.core.context.MutableExecutionContext;
import io.gravitee.gateway.reactive.core.processor.ProcessorChain;
import io.gravitee.gateway.reactive.core.v4.endpoint.EndpointManager;
import io.gravitee.gateway.reactive.handlers.api.v4.processor.MessageApiProcessorChainFactory;
import io.gravitee.node.api.Node;
import io.gravitee.node.api.configuration.Configuration;
import io.gravitee.plugin.entrypoint.EntrypointConnectorPluginManager;
import io.reactivex.rxjava3.core.Completable;

/**
 * Dedicated reactor for message APIs.
 */
public class MessageApiReactor extends TcpApiReactor {

    private final ProcessorChain beforeApiExecutionProcessors;
    private final ProcessorChain afterApiExecutionProcessors;
    private final ProcessorChain onErrorProcessors;

    public MessageApiReactor(
        final Api api,
        final Node node,
        final Configuration configuration,
        final DeploymentContext deploymentContext,
        final EntrypointConnectorPluginManager entrypointConnectorPluginManager,
        final EndpointManager endpointManager,
        final MessageApiProcessorChainFactory messageApiProcessorChainFactory,
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
        this.beforeApiExecutionProcessors = messageApiProcessorChainFactory.beforeApiExecution(api);
        this.afterApiExecutionProcessors = messageApiProcessorChainFactory.afterApiExecution(api);
        this.onErrorProcessors = messageApiProcessorChainFactory.onError(api);
    }

    @Override
    protected Completable beforeEntrypointRequest(MutableExecutionContext ctx) {
        return executeProcessorChain(ctx, beforeApiExecutionProcessors, ExecutionPhase.MESSAGE_REQUEST);
    }

    @Override
    protected Completable afterEndpointInvocation(MutableExecutionContext ctx) {
        return executeProcessorChain(ctx, afterApiExecutionProcessors, ExecutionPhase.MESSAGE_RESPONSE);
    }

    @Override
    protected Completable onError(MutableExecutionContext ctx) {
        return executeProcessorChain(ctx, onErrorProcessors, ExecutionPhase.MESSAGE_RESPONSE);
    }

    @Override
    public String toString() {
        return "MessageApiReactor API id[" + api.getId() + "] name[" + api.getName() + "] version[" + api.getApiVersion() + ']';
    }
}

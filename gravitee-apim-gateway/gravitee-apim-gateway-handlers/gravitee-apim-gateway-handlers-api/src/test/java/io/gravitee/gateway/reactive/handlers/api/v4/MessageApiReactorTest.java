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

import static io.gravitee.gateway.handlers.api.ApiReactorHandlerFactory.REPORTERS_LOGGING_EXCLUDED_RESPONSE_TYPES_PROPERTY;
import static io.gravitee.gateway.handlers.api.ApiReactorHandlerFactory.REPORTERS_LOGGING_MAX_SIZE_PROPERTY;
import static io.gravitee.gateway.reactive.handlers.api.v4.DefaultApiReactor.PENDING_REQUESTS_TIMEOUT_PROPERTY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.gravitee.definition.model.v4.ApiType;
import io.gravitee.gateway.env.RequestTimeoutConfiguration;
import io.gravitee.gateway.opentelemetry.TracingContext;
import io.gravitee.gateway.reactive.api.ExecutionPhase;
import io.gravitee.gateway.reactive.api.connector.entrypoint.BaseEntrypointConnector;
import io.gravitee.gateway.reactive.api.context.InternalContextAttributes;
import io.gravitee.gateway.reactive.core.context.DefaultDeploymentContext;
import io.gravitee.gateway.reactive.core.context.MutableExecutionContext;
import io.gravitee.gateway.reactive.core.context.MutableResponse;
import io.gravitee.gateway.reactive.core.processor.ProcessorChain;
import io.gravitee.gateway.reactive.core.v4.endpoint.EndpointManager;
import io.gravitee.gateway.reactive.core.v4.entrypoint.DefaultEntrypointConnectorResolver;
import io.gravitee.gateway.reactive.core.v4.invoker.TcpEndpointInvoker;
import io.gravitee.gateway.reactive.handlers.api.v4.processor.MessageApiProcessorChainFactory;
import io.gravitee.node.api.Node;
import io.gravitee.node.api.configuration.Configuration;
import io.gravitee.plugin.entrypoint.EntrypointConnectorPluginManager;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.CompletableObserver;
import java.util.Date;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
class MessageApiReactorTest {

    @Mock
    Configuration configuration;

    @Mock
    Node node;

    @Mock
    private Api api;

    @Mock
    private io.gravitee.definition.model.v4.Api apiDefinition;

    @Mock
    private DefaultEntrypointConnectorResolver entrypointConnectorResolver;

    @Mock
    private EntrypointConnectorPluginManager entrypointConnectorPluginManager;

    @Mock
    private EndpointManager endpointManager;

    @Mock
    private TcpEndpointInvoker defaultInvoker;

    @Mock
    private MutableExecutionContext ctx;

    @Mock
    private MutableResponse response;

    @Mock
    private BaseEntrypointConnector entrypointConnector;

    @Mock
    private MessageApiProcessorChainFactory messageApiProcessorChainFactory;

    @Mock
    private ProcessorChain beforeApiExecutionProcessors;

    @Mock
    private ProcessorChain afterApiExecutionProcessors;

    @Mock
    private ProcessorChain onErrorProcessors;

    @Mock
    private RequestTimeoutConfiguration requestTimeoutConfiguration;

    @Spy
    Completable spyBeforeProcessors = Completable.complete();

    @Spy
    Completable spyEntrypointRequest = Completable.complete();

    @Spy
    Completable spyInvokerChain = Completable.complete();

    @Spy
    Completable spyAfterProcessors = Completable.complete();

    @Spy
    Completable spyEntrypointResponse = Completable.complete();

    @Spy
    Completable spyResponseEnd = Completable.complete();

    private MessageApiReactor cut;

    @BeforeEach
    void init() throws Exception {
        lenient().when(response.end(any())).thenReturn(spyResponseEnd);
        lenient().when(ctx.response()).thenReturn(response);

        lenient().when(api.getDefinition()).thenReturn(apiDefinition);
        lenient().when(api.getId()).thenReturn("api-id");
        lenient().when(api.getName()).thenReturn("api-name");
        lenient().when(api.getDeployedAt()).thenReturn(new Date());
        lenient().when(api.getOrganizationId()).thenReturn("org-id");
        lenient().when(api.getEnvironmentId()).thenReturn("env-id");
        lenient().when(apiDefinition.getType()).thenReturn(ApiType.MESSAGE);

        lenient().when(defaultInvoker.invoke(any())).thenReturn(spyInvokerChain);
        lenient().when(ctx.getInternalAttribute(InternalContextAttributes.ATTR_INTERNAL_INVOKER)).thenReturn(defaultInvoker);
        lenient().when(entrypointConnector.handleRequest(ctx)).thenReturn(spyEntrypointRequest);
        lenient().when(entrypointConnector.handleResponse(ctx)).thenReturn(spyEntrypointResponse);
        lenient().when(entrypointConnectorResolver.resolve(ctx)).thenReturn(entrypointConnector);
        lenient()
            .when(ctx.getInternalAttribute(InternalContextAttributes.ATTR_INTERNAL_ENTRYPOINT_CONNECTOR))
            .thenReturn(entrypointConnector);

        lenient().when(beforeApiExecutionProcessors.execute(ctx, ExecutionPhase.MESSAGE_REQUEST)).thenReturn(spyBeforeProcessors);
        lenient().when(afterApiExecutionProcessors.execute(ctx, ExecutionPhase.MESSAGE_RESPONSE)).thenReturn(spyAfterProcessors);
        lenient().when(onErrorProcessors.execute(ctx, ExecutionPhase.MESSAGE_RESPONSE)).thenReturn(Completable.complete());

        when(messageApiProcessorChainFactory.beforeApiExecution(api)).thenReturn(beforeApiExecutionProcessors);
        when(messageApiProcessorChainFactory.afterApiExecution(api)).thenReturn(afterApiExecutionProcessors);
        when(messageApiProcessorChainFactory.onError(api)).thenReturn(onErrorProcessors);

        when(configuration.getProperty(REPORTERS_LOGGING_EXCLUDED_RESPONSE_TYPES_PROPERTY, String.class, null)).thenReturn(null);
        when(configuration.getProperty(REPORTERS_LOGGING_MAX_SIZE_PROPERTY, String.class, null)).thenReturn(null);
        when(configuration.getProperty(PENDING_REQUESTS_TIMEOUT_PROPERTY, Long.class, 10_000L)).thenReturn(10_000L);
        lenient().when(requestTimeoutConfiguration.getRequestTimeout()).thenReturn(0L);

        cut = new MessageApiReactor(
            api,
            node,
            configuration,
            new DefaultDeploymentContext(),
            entrypointConnectorPluginManager,
            endpointManager,
            messageApiProcessorChainFactory,
            requestTimeoutConfiguration,
            TracingContext.noop()
        );

        ReflectionTestUtils.setField(cut, "entrypointConnectorResolver", entrypointConnectorResolver);
        ReflectionTestUtils.setField(cut, "defaultInvoker", defaultInvoker);
        cut.doStart();
    }

    @Test
    void should_execute_message_processor_chains() {
        cut.handle(ctx).test().assertComplete();

        InOrder inOrder = inOrder(
            spyBeforeProcessors,
            spyEntrypointRequest,
            spyInvokerChain,
            spyAfterProcessors,
            spyEntrypointResponse,
            spyResponseEnd
        );

        inOrder.verify(spyBeforeProcessors).subscribe(any(CompletableObserver.class));
        inOrder.verify(spyEntrypointRequest).subscribe(any(CompletableObserver.class));
        inOrder.verify(spyInvokerChain).subscribe(any(CompletableObserver.class));
        inOrder.verify(spyAfterProcessors).subscribe(any(CompletableObserver.class));
        inOrder.verify(spyEntrypointResponse).subscribe(any(CompletableObserver.class));
        inOrder.verify(spyResponseEnd).subscribe(any(CompletableObserver.class));

        verify(beforeApiExecutionProcessors).execute(ctx, ExecutionPhase.MESSAGE_REQUEST);
        verify(afterApiExecutionProcessors).execute(ctx, ExecutionPhase.MESSAGE_RESPONSE);
    }
}

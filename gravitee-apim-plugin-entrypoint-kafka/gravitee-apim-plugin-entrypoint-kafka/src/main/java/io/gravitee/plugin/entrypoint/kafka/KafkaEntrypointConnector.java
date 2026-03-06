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

import io.gravitee.gateway.api.buffer.Buffer;
import io.gravitee.gateway.reactive.api.ApiType;
import io.gravitee.gateway.reactive.api.ConnectorMode;
import io.gravitee.gateway.reactive.api.ListenerType;
import io.gravitee.gateway.reactive.api.connector.entrypoint.async.EntrypointAsyncConnector;
import io.gravitee.gateway.reactive.api.context.ExecutionContext;
import io.gravitee.gateway.reactive.api.message.DefaultMessage;
import io.gravitee.gateway.reactive.api.message.Message;
import java.nio.charset.StandardCharsets;
import io.gravitee.gateway.reactive.api.qos.Qos;
import io.gravitee.gateway.reactive.api.qos.QosRequirement;
import io.gravitee.plugin.entrypoint.kafka.configuration.KafkaEntrypointConnectorConfiguration;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.vertx.kafka.client.serialization.BufferDeserializer;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.kafka.client.consumer.KafkaConsumer;
import io.vertx.rxjava3.kafka.client.consumer.KafkaConsumerRecord;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class KafkaEntrypointConnector extends EntrypointAsyncConnector {

    static final ApiType SUPPORTED_API = ApiType.MESSAGE;
    static final Set<ConnectorMode> SUPPORTED_MODES = Set.of(ConnectorMode.SUBSCRIBE);
    static final Set<Qos> SUPPORTED_QOS = Set.of(Qos.values());
    static final ListenerType SUPPORTED_LISTENER_TYPE = ListenerType.KAFKA;
    private static final String ENTRYPOINT_ID = "kafka-message";

    private final KafkaEntrypointConnectorConfiguration configuration;
    private KafkaConsumer<String, io.vertx.core.buffer.Buffer> consumer;

    public KafkaEntrypointConnector(KafkaEntrypointConnectorConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public String id() {
        return ENTRYPOINT_ID;
    }

    @Override
    public ListenerType supportedListenerType() {
        return SUPPORTED_LISTENER_TYPE;
    }

    @Override
    public Set<ConnectorMode> supportedModes() {
        return SUPPORTED_MODES;
    }

    @Override
    public int matchCriteriaCount() {
        return 0;
    }

    @Override
    public boolean matches(final ExecutionContext ctx) {
        return true;
    }

    @Override
    public Completable handleRequest(final ExecutionContext ctx) {
        Vertx vertx = ctx.getComponent(Vertx.class);
        Map<String, String> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getBootstrapServers());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BufferDeserializer.class.getName());
        if (configuration.getConsumerGroup() != null) {
            config.put(ConsumerConfig.GROUP_ID_CONFIG, configuration.getConsumerGroup());
        }
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());

        if (configuration.getSecurityProtocol() != null) {
            config.put("security.protocol", configuration.getSecurityProtocol());
        }
        if (configuration.getSaslMechanism() != null) {
            config.put("sasl.mechanism", configuration.getSaslMechanism());
        }
        if (configuration.getUsername() != null && configuration.getPassword() != null) {
            config.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" +
                configuration.getUsername() + "\" password=\"" + configuration.getPassword() + "\";"
            );
        }
        consumer = KafkaConsumer.create(vertx, config);
        Flowable<Message> messages = consumer
            .rxSubscribe(new HashSet<>(configuration.getTopics()))
            .andThen(consumer.toFlowable().map(this::toMessage));
        ctx.response().messages(messages);
        return Completable.complete();
    }

    private Message toMessage(KafkaConsumerRecord<String, io.vertx.core.buffer.Buffer> record) {
        final var metadata = new LinkedHashMap<String, Object>();
        metadata.put("topic", record.topic());
        metadata.put("partition", record.partition());
        metadata.put("offset", record.offset());
        metadata.put("timestamp", record.timestamp());

        final var headers = new LinkedHashMap<String, String>();
        record.headers().forEach(header -> {
            if (header.value() != null) {
                headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
        });

        return DefaultMessage
            .builder()
            .id(record.key())
            .content(Buffer.buffer(record.value().getBytes()))
            .metadata(metadata)
            .headers(headers)
            .build();
    }

    @Override
    public Completable handleResponse(final ExecutionContext executionContext) {
        return Completable.complete();
    }

    @Override
    public QosRequirement qosRequirement() {
        return QosRequirement.builder().build();
    }
}

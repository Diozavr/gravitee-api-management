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
package io.gravitee.apim.integration.tests.messages.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.gravitee.apim.gateway.tests.sdk.annotations.DeployApi;
import io.gravitee.apim.gateway.tests.sdk.annotations.GatewayTest;
import io.gravitee.apim.gateway.tests.sdk.connector.EntrypointBuilder;
import io.gravitee.apim.integration.tests.messages.AbstractKafkaEndpointIntegrationTest;
import io.gravitee.plugin.entrypoint.EntrypointConnectorPlugin;
import io.gravitee.plugin.entrypoint.kafka.KafkaEntrypointConnectorFactory;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.kafka.client.consumer.KafkaConsumer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

@GatewayTest
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@DeployApi({ "/apis/v4/messages/kafka-message/kafka-entrypoint-kafka-endpoint.json" })
class KafkaEntrypointKafkaEndpointIntegrationTest extends AbstractKafkaEndpointIntegrationTest {

    @Override
    public void configureEntrypoints(Map<String, EntrypointConnectorPlugin<?, ?>> entrypoints) {
        entrypoints.putIfAbsent("kafka-message", EntrypointBuilder.build("kafka-message", KafkaEntrypointConnectorFactory.class));
    }

    @Test
    void should_receive_messages_from_kafka(Vertx vertx) {
        KafkaConsumer<String, Buffer> consumer = getKafkaConsumer(vertx);
        TestSubscriber<Buffer> obs = subscribeToKafka(consumer).map(record -> Buffer.buffer(record.value())).test();
        obs.awaitCount(0, TimeUnit.MILLISECONDS); // just subscribe
        consumer.close();
        assertThat(obs.getCompletions()).isZero();
    }
}

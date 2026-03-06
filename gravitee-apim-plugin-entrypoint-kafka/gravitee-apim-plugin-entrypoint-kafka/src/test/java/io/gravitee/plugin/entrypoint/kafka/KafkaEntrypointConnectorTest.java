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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.gravitee.plugin.entrypoint.kafka.configuration.KafkaEntrypointConnectorConfiguration;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaHeader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaEntrypointConnectorTest {

    @Mock
    KafkaConsumerRecord<String, io.vertx.core.buffer.Buffer> record;

    @Mock
    KafkaHeader header;

    @Test
    void should_map_kafka_record_to_message_with_metadata_and_headers() throws Exception {
        when(record.key()).thenReturn("my-key");
        when(record.value()).thenReturn(io.vertx.core.buffer.Buffer.buffer("payload"));
        when(record.topic()).thenReturn("test-topic");
        when(record.partition()).thenReturn(1);
        when(record.offset()).thenReturn(42L);
        when(record.timestamp()).thenReturn(123456L);
        when(record.headers()).thenReturn(List.of(header));
        when(header.key()).thenReturn("h1");
        when(header.value()).thenReturn("v1".getBytes(StandardCharsets.UTF_8));

        KafkaEntrypointConnector connector = new KafkaEntrypointConnector(KafkaEntrypointConnectorConfiguration.builder().build());
        Method method = KafkaEntrypointConnector.class.getDeclaredMethod("toMessage", KafkaConsumerRecord.class);
        method.setAccessible(true);

        var message = (io.gravitee.gateway.reactive.api.message.Message) method.invoke(connector, record);

        assertThat(message.id()).isEqualTo("my-key");
        assertThat(message.content().toString()).isEqualTo("payload");
        assertThat(message.metadata())
            .containsEntry("topic", "test-topic")
            .containsEntry("partition", 1)
            .containsEntry("offset", 42L)
            .containsEntry("timestamp", 123456L);
        assertThat(message.headers()).containsEntry("h1", "v1");
    }
}

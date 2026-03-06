# RFC: Kafka Consumer Plugin for APIM Gateway (V4 Message APIs)

## 1. Summary

Реализовать полноценный Kafka consumer execution path в APIM Gateway для V4 `MESSAGE` API: Gateway читает сообщения из Kafka, запускает flow/policy chain на каждое сообщение, фиксирует измеримый результат (лог/метрика/trace), и управляет delivery semantics (ack/retry/DLQ).

Текущее состояние в ветке (обновлено):
- есть routing в отдельный `MessageApiReactorFactory` для `ApiType.MESSAGE` + listeners `SUBSCRIPTION/KAFKA`;
- reactor wiring добавлен в Spring;
- реализован `MessageApiProcessorChainFactory` и подключён в `MessageApiReactor`;
- в `TcpApiReactor` добавлены extension hooks для message-specific pipeline (`beforeEntrypointRequest`, `afterEndpointInvocation`, `onError`);
- в Kafka entrypoint добавлено обогащение message metadata (topic/partition/offset/timestamp + headers) и baseline config `enable.auto.commit=false`.

---

## 2. Goals

1. Gateway в роли Kafka consumer обрабатывает сообщения через flow/policy.
2. Policy execution даёт наблюдаемый side-effect.
3. Поддерживается надёжная стратегия commit/retry/DLQ.
4. Имеется real e2e тест с реальным Kafka broker.

## 3. Non-Goals

- Exactly-once semantics в первой итерации.
- Полный parity со всеми enterprise kafka policies в MVP.
- Переработка HTTP/TCP pipeline вне message path.

---

## 4. Target Architecture

### 4.1 Components
- `MessageApiReactor` (отдельный execution pipeline, не просто wrapper).
- `MessageApiProcessorChainFactory` (before/after/security/error chains).
- Kafka Entrypoint Connector (consumer-side ingest).
- Kafka Endpoint Connector (publish/DLQ path).
- Message Execution Context (topic/partition/offset/key/headers).
- Observability hooks (metrics/logging/tracing).

### 4.2 Phase Mapping (предлагаемая)
- `CONSUME_INIT` — получение рекорда и обогащение context.
- `MESSAGE_REQUEST` — policy pre-processing.
- `MESSAGE_RESPONSE` — post-processing + decision commit/retry.
- `ERROR` — error processors + DLQ decision.

---

## 5. Delivery Semantics (MVP)

- **Baseline**: at-least-once.
- Commit offset только после успешного выполнения flow/policies.
- Ошибки:
  - retriable: retry с backoff и лимитом попыток;
  - non-retriable: DLQ publish + mark failed.

---

## 6. Observability Contract

Обязательные метрики:
- `kafka_consume_total`
- `kafka_process_success_total`
- `kafka_process_error_total`
- `kafka_retry_total`
- `kafka_dlq_total`
- `kafka_process_latency_ms`

Структурированные логи:
- `apiId`, `planId`, `topic`, `partition`, `offset`, `policyId`, `outcome`, `attempt`.

Tracing:
- span `kafka.consume`
- child span `message.flow`
- child spans per policy execution

---

## 7. Security & Isolation

- Поддержка Kafka auth (SASL/SSL) через конфиг connectors.
- Tenant isolation через existing endpoint manager constraints.
- Плановые ограничения (`plan`, `subscription`) в message chain.

---

## 8. Implementation Plan (Phased)

### Phase A — Core Execution Context
- [ ] Ввести `MessageExecutionContext`/internal attributes.
- [~] Добавить mapping KafkaRecord -> context (частично: metadata/headers на `Message` добавлены, отдельный execution context ещё не введён).

**DoD**: unit tests на все ключевые поля.
**Статус**: **частично выполнено**.

### Phase B — Message Processor Chain
- [x] Реализовать `MessageApiProcessorChainFactory`.
- [x] Подключить в `MessageApiReactor`.

**DoD**: порядок chain вызовов тестами подтверждён.
**Статус**: **выполнено**.

### Phase C — Policy/Flow Integration
- [~] Подключить phase mapping `MESSAGE_REQUEST/RESPONSE` (processor phase wiring сделан, полная flow/policy интеграция требует доработки).
- [ ] Гарантировать policy execution per message.

**DoD**: integration test показывает side-effect policy.
**Статус**: **частично выполнено**.

### Phase D — Commit/Retry/DLQ
- [ ] Реализовать retry policy + DLQ publish.
- [~] Commit только после success path (baseline auto-commit disabled, но полноценная explicit commit/retry/DLQ логика не завершена).

**DoD**: integration tests success/retry/dlq.
**Статус**: **не выполнено / частично выполнено**.

### Phase E — E2E
- [ ] Реальный Kafka broker (без эмуляции).
- [ ] Gateway consumer path end-to-end.

**DoD**: тест воспроизводимо green.
**Статус**: **не выполнено**.

---

## 9. E2E Acceptance Scenario (канонический)

1. Поднять Kafka.
2. Деплоить MESSAGE API с Kafka listener + policy.
3. Отправить message в topic.
4. Убедиться, что Gateway:
   - прочитал message,
   - выполнил policy,
   - записал metric/log/trace,
   - корректно обработал offset.

---

## 10. Risks & Mitigations

- **Risk**: commit до завершения policy -> потеря semantics.
  **Mitigation**: commit only after post-flow success.

- **Risk**: rebalance при in-flight message.
  **Mitigation**: pause/drain protocol + graceful shutdown hooks.

- **Risk**: policy latency повышает lag.
  **Mitigation**: bounded concurrency + lag monitoring.

---

## 11. Rollout Strategy

- Feature flag: `gateway.message.kafka.consumer.enabled`.
- Canary rollout по environment/tag.
- Быстрый rollback: выключение флага и возврат к текущему behavior.

---

## 12. Open Questions

1. Нужен ли early support for transactional consumer (EOS)?
2. Какая default retry policy (count/backoff/jitter)?
3. Где хранить failure metadata для DLQ replay?
4. Требуется ли per-plan QoS policy для Kafka consumption?

---

## 13. Success Criteria

- 99%+ успешных обработок в номинальном потоке.
- Отсутствие silent loss при graceful shutdown.
- Наблюдаемость покрывает root-cause за < 5 мин.

---

## 14. Sprint Backlog (предлагаемый)

### Sprint 1
- [ ] MessageExecutionContext contract + tests.
- [x] MessageApiProcessorChainFactory skeleton.
- [x] MessageApiReactor: separate chain (не только wrapper).
- [ ] Happy path flow/policy invocation test.

### Sprint 2
- [ ] Kafka commit/retry baseline.
- [ ] Structured logging + base metrics.
- [ ] Real Kafka e2e (single broker, one topic, one policy side effect).

### Sprint 3
- [ ] DLQ + rebalance hardening.
- [ ] Perf and soak tests.
- [~] Rollout flags + docs (добавлен feature flag `gateway.message.kafka.consumer.enabled`, остальная rollout-документация требует доработки).

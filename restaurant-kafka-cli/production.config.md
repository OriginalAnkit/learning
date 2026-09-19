# Kafka Configuration Guide: Don't Lose Messages

A simple guide to the Kafka settings that protect your messages, explained with a **restaurant order app** analogy.

## Where messages can be lost

```
Producer  ──►  Broker  ──►  Consumer
(order app)   (Kafka)      (kitchen)
```

Messages can be lost at any of these three points, so each one needs the right configuration.

---

## Quick Cheat Sheet

| Config | Where | Simple meaning | Safe value |
|---|---|---|---|
| `acks` | Producer | Who must confirm the write | `all` |
| `retries` | Producer | Try again if sending fails | High number |
| `enable.idempotence` | Producer | No duplicates on retry | `true` |
| `replication.factor` | Topic | Number of copies | `3` |
| `min.insync.replicas` | Topic | Minimum copies that must confirm | `2` |
| `retention.ms` | Topic | How long messages are kept | Long enough for your consumers |
| `unclean.leader.election.enable` | Broker | Allow out-of-date server as leader | `false` |
| `enable.auto.commit` | Consumer | Auto-save bookmark (offset) | `false` |
| `auto.offset.reset` | Consumer | Where to start with no bookmark | `earliest` |
| `group.id` | Consumer | Name of the consumer group | Your service name |
| `transactional.id` | Producer | Exactly-once across messages | Only if needed |

---

## Producer Configs (the app sending orders)

### 1. `acks`
How many confirmations the producer waits for before saying "order placed."

| Value | Meaning | Risk |
|---|---|---|
| `0` | Don't wait at all | Fastest, but orders can be lost |
| `1` | Wait for the leader only | Lost if the leader crashes right after |
| `all` | Wait for all in-sync backup copies | Safest |

### 2. `retries`
If sending fails, how many times to try again. Set it high so a small network problem doesn't lose an order.

### 3. `enable.idempotence=true`
Stops duplicates caused by retries. If the first attempt actually worked, Kafka notices the retry and ignores it.

---

## Topic Configs (the order board)

### 4. `replication.factor`
How many copies of each partition are stored on different servers.
- `3` means 3 copies. If one server dies, the data is still safe.

### 5. `min.insync.replicas`
The minimum number of copies that must be up to date for a write to succeed.
- With `acks=all` and this set to `2`, at least 2 servers must confirm the order.
- If fewer are available, Kafka **rejects** the write instead of storing it unsafely.

### 6. `retention.ms`
How long Kafka keeps messages before deleting them.
- Set it long enough that consumers can still read messages after downtime (for example, a weekend outage).

---

## Broker Config (the Kafka server)

### 7. `unclean.leader.election.enable=false`
When a leader server dies, a backup takes over. This setting means only a backup that has **all** the data can become the leader.
- If `true`, an out-of-date backup could take over and messages would be lost silently.

---

## Consumer Configs (the chef reading orders)

### 8. `enable.auto.commit=false`
By default, Kafka saves the consumer's bookmark (offset) automatically every few seconds, even if processing isn't finished. Turning this off means you save the bookmark yourself.

### 9. Manual commit after processing
Not a config, but a habit: **process first, then commit.**
- Crash before commit: the message is read again (safe).
- Commit before processing, then crash: the message is lost.

### 10. `auto.offset.reset`
What a consumer does when it has no bookmark, such as a brand-new group.
- `earliest`: start from the oldest message.
- `latest`: start from new messages only.

### 11. `group.id`
The name of the consumer group. Consumers with the same name share the work, and Kafka tracks one bookmark per group.

---

## Advanced (Optional)

### 12. `transactional.id`
Enables **exactly-once** processing across multiple messages, so they all succeed or none do. It adds complexity, so use it only when you really need it.

---

## Example Setup

### Producer

```properties
acks=all
retries=2147483647
enable.idempotence=true
```

### Topic

```bash
kafka-topics.sh --create \
  --topic pizza-orders \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000 \
  --bootstrap-server localhost:9092
```

### Broker (`server.properties`)

```properties
unclean.leader.election.enable=false
```

### Consumer

```properties
group.id=kitchen-team
enable.auto.commit=false
auto.offset.reset=earliest
```

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'pizza-orders',
    group_id='kitchen-team',
    enable_auto_commit=False,
    auto_offset_reset='earliest'
)

for msg in consumer:
    cook_pizza(msg.value)   # 1. process first
    consumer.commit()       # 2. then commit the offset
```

---

## Extra Good Practices

- **Idempotent processing:** Handling the same message twice should be harmless (for example, skip an `order_id` you've already processed).
- **Dead letter topic:** Send messages that always fail to a separate topic (`pizza-orders-dlq`) instead of blocking or dropping them.
- **Monitor consumer lag:** Growing lag means messages are piling up.
- **Monitor under-replicated partitions:** This signals a broker problem.

---

## Easy Way to Remember

> `acks=all` + replication factor 3 + `min.insync.replicas=2` protects against **server failures**.
>
> Manual commit after processing protects against **consumer crashes**.

Losing messages almost always comes from weak defaults (`acks=1`, auto-commit), not from Kafka itself.
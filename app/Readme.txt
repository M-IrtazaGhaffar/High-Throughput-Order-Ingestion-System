PRODUCTION-GRADE RABBITMQ + MONGODB INGESTION SYSTEM
====================================================

OVERVIEW
--------

This system ingests high-volume order items (1000–10,000/sec) safely.

It guarantees:
- No data loss
- No duplicate inserts
- Controlled retries
- Exponential backoff
- Final Dead Letter Queue
- Crash safety
- Horizontal scalability

ARCHITECTURE
------------

Client
  ↓
API (Node.js)
  ↓
RabbitMQ (Main Queue: order_items)
  ↓
Worker (Batch processing)
  ↓
MongoDB (order_items collection)

RETRY FLOW
----------

If Mongo write fails:

1st failure → retry_queue_1 (5 sec delay)
2nd failure → retry_queue_2 (15 sec delay)
3rd failure → retry_queue_3 (30 sec delay)
4th failure → Final DLQ

No infinite loops.
No retry storms.
Controlled exponential backoff.

CRASH SAFETY
------------

If API crashes:
- Message already in Rabbit → safe.

If Worker crashes:
- Unacked messages requeued automatically.

If Mongo crashes:
- Worker does not ACK.
- Messages retried later.

If Rabbit crashes:
- Durable queue + persistent messages restore safely.

DUPLICATE PROTECTION
--------------------

Mongo has unique index on _id.
If same message replays → insert ignored.

DELIVERY MODEL
--------------

At-Least-Once Delivery + Idempotent Writes.

Exactly-once does not exist in distributed systems.
This is the industry standard.

END.
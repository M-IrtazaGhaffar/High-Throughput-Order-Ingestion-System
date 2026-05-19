const express = require("express");
const amqp = require("amqplib");
const { MongoClient } = require("mongodb");
const { v4: uuidv4 } = require("uuid");

const app = express();
app.use(express.json());

/* CONFIG */

const BATCH_SIZE = 500;
const FLUSH_INTERVAL = 100;
const ARTIFICIAL_DB_DELAY = 0; // ← In Production set it to 0

/* STATE */

let channel;
let collection;
let buffer = [];
let messages = [];
let isFlushing = false;
let shuttingDown = false;

/* INIT */

async function init() {

    /* Mongo */

    const mongoClient = new MongoClient(process.env.MONGO_URI, {
        maxPoolSize: 20
    });

    await mongoClient.connect();
    collection = mongoClient.db("test").collection("order_items");

    await collection.createIndex({ orderId: 1 });

    /* Rabbit */

    const conn = await amqp.connect(process.env.RABBIT_URL);
    channel = await conn.createChannel();

    /* Final DLX */

    await channel.assertExchange("final_dlx", "direct", { durable: true });

    await channel.assertQueue("order_items_dlq", { durable: true });

    await channel.bindQueue(
        "order_items_dlq",
        "final_dlx",
        "dead"
    );

    /* Retry Exchange */

    await channel.assertExchange("retry_exchange", "direct", { durable: true });

    /* Retry Queues */

    await createRetryQueue("retry_queue_1", 5000, "retry_1");
    await createRetryQueue("retry_queue_2", 15000, "retry_2");
    await createRetryQueue("retry_queue_3", 30000, "retry_3");

    /* Main Queue */

    await channel.assertQueue("order_items", {
        durable: true,
        arguments: {
            "x-dead-letter-exchange": "final_dlx",
            "x-dead-letter-routing-key": "dead",
            "x-queue-mode": "lazy"
        }
    });

    channel.prefetch(BATCH_SIZE);

    channel.consume("order_items", onMessage, { noAck: false });

    setInterval(flush, FLUSH_INTERVAL);

    console.log("✅ Server started");
}

/* RETRY QUEUE CREATION */

async function createRetryQueue(name, ttl, routingKey) {

    await channel.assertQueue(name, {
        durable: true,
        arguments: {
            "x-message-ttl": ttl,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": "order_items"
        }
    });

    await channel.bindQueue(name, "retry_exchange", routingKey);
}

/* API */

app.post("/add-item", async (req, res) => {

    const { orderId, payload } = req.body;

    const item = {
        _id: uuidv4(),
        orderId,
        payload,
        createdAt: new Date()
    };

    channel.sendToQueue(
        "order_items",
        Buffer.from(JSON.stringify(item)),
        { persistent: true }
    );

    res.status(202).json({ status: "queued" });
});

/* DLQ REPLAY */

app.post("/replay-dlq", async (req, res) => {

    const limit = parseInt(req.query.limit) || 1000;
    const batch = [];
    const msgs = [];

    while (batch.length < limit) {

        const msg = await channel.get("order_items_dlq", { noAck: false });

        if (!msg) break;

        batch.push(msg.content);
        msgs.push(msg);
    }

    if (batch.length === 0) {
        return res.json({ status: "empty", replayed: 0 });
    }

    for (const content of batch) {
        channel.sendToQueue(
            "order_items",
            content,
            {
                persistent: true,
                headers: { "x-retry-count": 0 }
            }
        );
    }

    msgs.forEach(m => channel.ack(m));

    console.log(`♻️ Replayed ${batch.length} messages from DLQ`);
    res.json({ status: "done", replayed: batch.length });
});

/* WORKER */

function onMessage(msg) {

    if (shuttingDown) return;

    try {

        const data = JSON.parse(msg.content.toString());

        buffer.push({ insertOne: { document: data } });
        messages.push(msg);

        if (buffer.length >= BATCH_SIZE) {
            flush();
        }

    } catch (err) {

        // JSON parse failed — bad message, send straight to DLQ
        console.error("Bad message, sending to DLQ:", err.message);
        channel.nack(msg, false, false); // requeue=false → DLQ
    }
}

/* FLUSH */

async function flush() {

    if (isFlushing || buffer.length === 0) return;

    isFlushing = true;

    const ops = buffer.splice(0);
    const msgs = messages.splice(0);

    try {

        await new Promise(r => setTimeout(r, ARTIFICIAL_DB_DELAY));

        // ← randomly fail 30% of the time to trigger retries
        // if (Math.random() < 0.3) {
        //     throw new Error("Simulated DB failure");
        // }

        await collection.bulkWrite(ops, { ordered: false });

        msgs.forEach(m => channel.ack(m));

        console.log(`Inserted ${ops.length}`);

    } catch (err) {

        console.log(`❌ Failed batch of ${ops.length}`);

        // ✅ Duplicate key error — unique index blocked it, safe to ack
        const isDuplicate = err.code === 11000 ||
            err.writeErrors?.every(e => e.code === 11000);

        if (isDuplicate) {
            msgs.forEach(m => channel.ack(m));
        } else {
            // Retryable error (Mongo down, timeout etc) — use backoff retry
            for (const m of msgs)
                await retryMessage(m);
        }

    } finally {
        isFlushing = false;
    }
}

/* RETRY HANDLER */

async function retryMessage(msg) {

    const headers = msg.properties.headers || {};
    const retryCount = headers["x-retry-count"] || 0;

    if (retryCount >= 3) {
        channel.nack(msg, false, false);
        console.log("Sent to DLQ");
        return;
    }

    const nextRetry = retryCount + 1;

    channel.publish(
        "retry_exchange",
        `retry_${nextRetry}`,
        msg.content,
        {
            persistent: true,
            headers: { "x-retry-count": nextRetry }
        }
    );

    channel.ack(msg);
}

/* GRACEFUL SHUTDOWN */

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

async function shutdown() {

    shuttingDown = true;

    await flush();

    await channel.close();

    process.exit(0);
}

/* START */

init().then(() => {
    app.listen(3000, () => {
        console.log("Listening on 3000");
    });
});
            

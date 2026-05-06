const autocannon = require("autocannon");

const items = ["product-a", "product-b", "product-c", "product-d", "product-e"];
const users = Array.from({ length: 1000 }, (_, i) => `user-${i + 1}`);

function randomBody() {
    const userId = users[Math.floor(Math.random() * users.length)];
    return JSON.stringify({
        orderId: `order-${userId}-${Math.random().toString(36).slice(2)}`,
        payload: {
            userId,
            item: items[Math.floor(Math.random() * items.length)],
            qty: Math.floor(Math.random() * 10) + 1,
            price: parseFloat((Math.random() * 100).toFixed(2)),
            timestamp: new Date().toISOString()
        }
    });
}

// pre-generate 5000 unique bodies
const requests = Array.from({ length: 5000 }, () => ({
    method: "POST",
    path: "/add-item",
    headers: { "Content-Type": "application/json" },
    body: randomBody()
}));

console.log("🚀 Simulating 100 users x 10k rows = 1M requests");
console.log("📊 Watch RabbitMQ: http://localhost:15672");
console.log("📦 Watch MongoDB:  http://localhost:8081\n");

const instance = autocannon({
    url: "http://localhost:3000",
    connections: 200,
    pipelining: 5,
    amount: 1_000_000,  // 100 users x 10k rows -> 1 Million Requests
    requests,
}, (err, result) => {
    if (err) return console.error(err);

    console.log("\n========== FINAL RESULTS ==========");
    console.log(`Total Requests:  ${result.requests.total.toLocaleString()}`);
    console.log(`Requests/sec:    ${result.requests.mean.toLocaleString()}`);
    console.log(`Latency avg:     ${result.latency.mean} ms`);
    console.log(`Latency p99:     ${result.latency.p99} ms`);
    console.log(`Throughput:      ${(result.throughput.mean / 1024 / 1024).toFixed(2)} MB/s`);
    console.log(`Errors:          ${result.errors.toLocaleString()}`);
    console.log(`Timeouts:        ${result.timeouts.toLocaleString()}`);
    console.log(`Duration:        ${(result.duration).toFixed(2)}s`);
    console.log("====================================");
});

autocannon.track(instance, {
    renderProgressBar: true,
    renderResultsTable: true,
    renderLatencyTable: true
});
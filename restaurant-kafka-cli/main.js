const readline = require("readline/promises");
const { Kafka } = require("kafkajs");


const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

rl.on("close", () => {
    console.log("\nBye!");
    process.exit(0);
});

const kafka = new Kafka({
    clientId: "Restaurant-app-1",
    brokers: ["192.168.1.39:9092"],
});

const events = {
    "NEW_ORDER": {
        name: "new-order",
        partition: 2
    },
    "ORDER_ACCEPTED": { name: "order-accepted", partition: 1 },
    "ORDER_READY": { name: "order-ready", partition: 2 },
}

//Creating kafka topics if not exist
const init = async () => {
    const admin = kafka.admin();
    console.log("Admin connecting...");
    admin.connect();
    console.log("Adming Connection Success...");

    const existing = await admin.listTopics();
    const events_to_be_created = []
    Object.keys(events).map((e) => {
        const event = events[e];
        if (!existing.includes(event.name)) {
            events_to_be_created.push({
                topic: event.name,
                numPartitions: event.partition
            });
        }
    })
    console.log(events_to_be_created);

    if (events_to_be_created.length > 0) {
        await admin.createTopics({
            topics: events_to_be_created,
        });
    }

    console.log("Disconnecting Admin..");
    await admin.disconnect();
}

// ====================== PRODUCER START======================
const orders = []
sendEvent = async (topic, partition, data) => {
    //connecting producer
    const producer = kafka.producer();
    await producer.connect();
    await producer.send({
        topic: topic,
        messages: [
            {
                partition: partition,
                value: JSON.stringify(data),
            },
        ],
    });
}

const producer = async () => {
    console.log('============ Working as producer ==================');
    await init();
    while (true) {
        const reply = await rl.question(`
            >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
            0 . Exit 
            1. for order list -> 1
            2. for new order -> 2/order_id/dish_name/amount
            3. for order accepted -> 3/order_id
            4. for order ready -> 4/order_id
            >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>`);
        const [option, order_id, dish_name, amount] = reply.split('/');
        switch (option) {
            case "0":
                return;
            case "1":
                console.log(orders);
                break;
            case "2":
                orders.push({ order_id, dish_name, amount, status: "NEW_ORDER" });
                await sendEvent(events.NEW_ORDER.name, orders.length % 2, orders[orders.length - 1]);
                break;
            case "3":
                let oi = orders.findIndex(o => o.order_id == order_id);
                orders[oi].status = "ORDER_ACCEPTED";
                await sendEvent(events.ORDER_ACCEPTED.name, oi % 2, orders[orders.length - 1]);

                break;
            case "4":
                let oai = orders.findIndex(o => o.order_id == order_id);
                orders[oai].status = "ORDER_READY";
                await sendEvent(events.ORDER_READY.name, oai % 2, orders[orders.length - 1]);
                break;
            default:
                console.log("Invalid choice, try again\n");
        }
    }
}

// ====================== PRODUCER END======================


// ====================== CONSUMER START======================
const riderConsumer = async () => {
    const group = 'rider'
    const riderConsumer = kafka.consumer({ groupId: group });
    await riderConsumer.connect();
    await riderConsumer.subscribe({ topics: [events.ORDER_READY.name], fromBeginning: true });
    await riderConsumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            console.log(
                `============\n ${group}: [${topic}]: PART:${partition}:`,
                message.value.toString(),
                "\n============"
            );
        },
    });
}
const billingConsumer = async () => {
    const group = 'billing'
    const riderConsumer = kafka.consumer({ groupId: group });
    await riderConsumer.connect();
    await riderConsumer.subscribe({ topics: [events.NEW_ORDER.name], fromBeginning: true });
    await riderConsumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            console.log(
                `============\nbiller consumer 1 ${group}: [${topic}]: PART:${partition}:`,
                message.value.toString(),
                "\n============"
            );
        },
    });

    const riderConsumer2 = kafka.consumer({ groupId: group });
    await riderConsumer2.connect();
    await riderConsumer2.subscribe({ topics: [events.NEW_ORDER.name], fromBeginning: true });
    await riderConsumer2.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            console.log(
                `============\nbiller consumer 2 ${group}: [${topic}]: PART:${partition}:`,
                message.value.toString(),
                "\n============"
            );
        },
    });
}

const notificationConsumer = async () => {
    group = 'notification'
    const notificationConsumer = kafka.consumer({ groupId: group });
    await notificationConsumer.connect();
    await notificationConsumer.subscribe({ topics: [events.NEW_ORDER.name, events.ORDER_ACCEPTED.name, events.ORDER_READY.name], fromBeginning: true });
    await notificationConsumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            console.log(
                `============\n notification consumer 2 ${group}: [${topic}]: PART:${partition}:`,
                message.value.toString(),
                "\n============"
            );
        },
    });

    const notificationConsumer2 = kafka.consumer({ groupId: group });
    await notificationConsumer2.connect();
    await notificationConsumer2.subscribe({ topics: [events.NEW_ORDER.name, events.ORDER_ACCEPTED.name, events.ORDER_READY.name], fromBeginning: true });
    await notificationConsumer2.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            console.log(
                `============\n notification consumer 2 ${group}: [${topic}]: PART:${partition}:`,
                message.value.toString(),
                "\n============"
            );
        },
    });
}

const consumer = async () => {
    console.log('============= Working as consumer ============= ');
    await init();

    // await Promise.all([
    riderConsumer(),
        billingConsumer(),
        notificationConsumer()
    // ]);

}
// ====================== CONSUMER END======================

async function main() {


    while (true) {
        const answer = await rl.question(
            "Who are you?\n 1. Producer\n 2. Consumer\n 3. Exit\n> "
        );

        switch (answer.trim()) {
            case "1":
                await producer();
                break;
            case "2":
                await consumer();
                break;
            case "3":
                rl.close(); // triggers the "close" handler, which prints Bye! and exits
                return;
            default:
                console.log("Invalid choice, try again\n");
        }
    }
}

main();
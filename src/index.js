const express = require("express");
const { Kafka, Partitioners } = require("kafkajs");
const { config } = require("dotenv");

config();
const app = express();

const kafkaConfig = new Kafka({
  clientId: "my-producer",
  brokers: ["localhost:9092"],
});

const runProducer = async () => {
  const producer = kafkaConfig.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  await producer.connect();
  for (let i = 0; i < 100; i++) {
    await producer.send({
      topic: "test-topics",
      messages: [
        {
          value: `Message ${i}`,
        },
      ],
    });
    if (i % 10 === 0) {
      console.log(`Produced ${i} messages`);
    }
  }

  //   await producer.disconnect();
};

const runConsumer = async () => {
  const consumer = kafkaConfig.consumer({ groupId: "test-group" });
  await consumer.connect();
  await consumer.subscribe({ topic: "test-topics", fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
    },
  });
  //   await consumer.disconnect();
};

app.listen(process.env.PORT, async () => {
  console.log("Server running on:", process.env.PORT);
  try {
    await runProducer();
    setInterval(async () => {
      await runProducer();
    }, 1 * 60 * 1000);
  } catch (error) {
    console.error("producer error", error);
  }

  try {
    await runConsumer();
  } catch (error) {
    console.error("consumer", error);
  }
});

const { Producer } = require("node-rdkafka");

//created topic name
const topicName = "purchases";

//Producer has initialized
//configuration are taken from "https://github.com/confluentinc/librdkafka/blob/v2.0.2/CONFIGURATION.md"
const producer = new Producer({
  "metadata.broker.list": "localhost:9092", // Connect to a Kafka instance on localhost and we can add multiple brokers sperated by commas.
  "compression.codec": "gzip", //compression codec to use for compressing message sets
  "retry.backoff.ms": 200, //The backoff time in milliseconds before retrying a protocol request.
  "message.send.max.retries": 10, //How many times to retry sending a failing Message.
  "socket.keepalive.enable": true, //Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets
  dr_cb: true, // Specifies that we want a delivery-report event to be generated
  dr_msg_cb: true, //used to get message payload with delivery report
});

//connecting to broker
producer.connect();

// The ready event is emitted when the Producer is ready to send messages.
// Wait for the ready event before proceeding
producer.on("ready", readyHandler);

//Any errors we encounter, including connection errors
producer.on("event.error", errorHandler);

//The delivery-report event is emitted when a delivery report has been found via polling.
//dr_msg_cb if you want the report to contain the message payload
producer.on("delivery-report", deliveryHandler);

// We must either call .poll() manually after sending messages
// or set the producer to poll on an interval (.setPollInterval).
// Without this, we do not get delivery events and the queue
// will eventually fill up.
producer.setPollInterval(100);

//ready handler
function readyHandler(data) {
  console.log({ level: "info", message: "Producer Ready", data });
  //sending message
  const message = {
    firstname: "Viraj",
    lastname: "Doshi",
  };

  sendMessage(message);
}

//error handler
function errorHandler(error) {
  console.error({ level: "error", message: "Error from producer", error });
}

//delivery report
function deliveryHandler(err, report) {
  if (err) {
    //message is buffer so converting into string
    let message = report.value.toString();

    //parsing into json
    message = JSON.parse(message);

    console.error({ level: "error", message: `Message is failed to delivered for ${message}`, error: err });
  }
  console.log({
    level: "info",
    message: "Delivery report",
    data: {
      topic: report.topic,
      partition: report.partition,
      offset: report.offset,
      size: report.size,
      timeStamp: report.timestamp,
    },
  });
}

//sending message to kafka queue
function sendMessage(message) {
  try {
    if (producer.isConnected()) {
      producer.produce(
        // Topic to send the message to
        topicName,

        // optionally we can manually specify a partition for the message
        // this defaults to -1 - which will use librdkafka's default partitioner (consistent random for keyed messages, random for unkeyed messages)
        null,

        // Message to send. Must be a buffer
        Buffer.from(JSON.stringify(message), "utf-8"),

        // for keyed messages, we also specify the key - note that this field is optional
        null,

        // you can send a timestamp here.
        // it will get added. Otherwise, we default to 0
        Date.now()
      );
    } else {
      console.log("producer not connected");
    }
  } catch (error) {
    console.error({ message: "A problem occurred when sending our message", error });
  }
}

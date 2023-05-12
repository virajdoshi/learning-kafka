const { KafkaConsumer, CODES } = require("node-rdkafka");

//created topic name
const topicName = "purchases";

// Consumer has initialized
// configuration are taken from "https://github.com/confluentinc/librdkafka/blob/v2.0.2/CONFIGURATION.md"
const consumer = new KafkaConsumer(
  {
    "group.id": "kafka", // All clients sharing the same group.id belong to the same group.
    "metadata.broker.list": "localhost:9092", // Connect to a Kafka instance on localhost and we can add multiple brokers sperated by commas.

    /**
    Rebalance is the re-assignment of partition ownership among consumers within a given consumer group. Remember that every consumer in a consumer group is assigned one or more topic partitions exclusively.
    A Rebalance happens when:
     -> a consumer JOINS the group
     -> a consumer SHUTS DOWN cleanly
     -> a consumer is considered DEAD by the group coordinator. This may happen after a crash or when the consumer is busy with a long-running processing, which means that no heartbeats has been sent in the meanwhile by the consumer to the group coordinator within the configured session interval
     -> new partitions are added
   */
    rebalance_cb: function (err, assignment) {
      console.log({ level: "info", message: assignment });

      if (err.code === CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
        this.assign(assignment);
      } else if (err.code == CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
        this.unassign();
      } else {
        console.error(err);
      }
    },
  },
  {
    // Automatically and periodically commit offsets in the background
    "enable.auto.commit": true,
  }
);

//connecting to broker
consumer.connect({}, function (err, data) {
  if (err) console.log(err);
  console.log({ level: "info", message: "consumer connected successfully" });
});

//The ready event is emitted when the Consumer is ready to read messages.
consumer.on("ready", readyHandler);

//Any errors we encounter, including connection errors
consumer.on("event.error", errorHandler);

//When using the Standard API consumed messages are emitted in this event.
consumer.on("data", dataHandler);

//ready handler
function readyHandler() {
  // Subscribe to the purchases topic
  // This makes subsequent consumes read from that topic.
  consumer.subscribe([topicName]);

  //consumes the data from queue
  consumer.consume();
}

//error handler
function errorHandler(error) {
  console.error({ level: "error", message: "Error from consumer", error });
}

//data handler
//recieves data in below format
/**
    {
        value: Buffer.from('hi'), // message contents as a Buffer
        size: 2, // size of the message, in bytes
        topic: 'librdtesting-01', // topic the message comes from
        offset: 1337, // offset the message was read from
        partition: 1, // partition the message was on
        key: 'someKey', // key of the message if present
        timestamp: 1510325354780 // timestamp of message creation
    }
 */
function dataHandler(data) {
  let { value, ...remaningData } = data;
  value = JSON.parse(data.value);
  console.log({ level: "info", value, ...remaningData });
}

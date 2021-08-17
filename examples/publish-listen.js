import Amqp from 'k6/x/amqp';
import Queue from 'k6/x/amqp/queue';

export default function () {
  console.log("K6 amqp extension enabled, version: " + Amqp.version)
  const url = "amqp://guest:guest@localhost:5672/"
  Amqp.start({
    connection_url: url
  })
  const queueName = 'K6 queue';
  const consumerName = 'K6 consumer';
  const replyToQueue = "a_reply_queue";
  const correlationId = "abcde";

  const publish = function (mark, queue, reply, correlation) {
    try {
      Amqp.publish({
        queue_name: queue,
        exchange: '',
        mandatory: false,
        immediate: false,
        body: "Ping from k6 -> " + mark,
        reply_to: reply,
        correlation_id: correlation
      })
    } catch (error) {
      console.log(`caught error ${error}`)
    }
  }

  Queue.declare({
    name: queueName,
    durable: false,
    delete_when_unused: false,
    exclusive: false,
    no_wait: false,
    args: null
  })

  Queue.declare({
    name: replyToQueue,
    durable: false,
    delete_when_unused: false,
    exclusive: false,
    no_wait: false,
    args: null
  })

  console.log(`queues are ready: ${queueName}, ${replyToQueue}`)

  console.log(queueName + " publishing...")
  publish('A', queueName, replyToQueue, correlationId);
  publish('B', queueName, replyToQueue, correlationId);
  publish('C', queueName, replyToQueue, correlationId);
  console.log(queueName + " published!")

  // need to pull from the replyTo to get the responses
  // need to actually respond 
  //   so, a listener on the target queue that can basically Amqp.publish() responses

  // TODO: This would happen at maven microservice level!
  const listener = function (data) {
    const dataChunks = data.split("|")
    const payload = dataChunks[0]
    const replyTo = dataChunks[1]
    const correlation = dataChunks[2]

    console.log(`payload: ${payload}; replyTo: ${replyTo}; correlation: ${correlation}`)

    // publish a "response" here
    // make sure we plug into the response queue and verify that we've gotten a reply

    // Note: The "RPC Guts" are being exposed here. I.e. we are managing replyTo and correlation ID manually
    // instead of an abstraction. A better approach would probably be to abstract the actual RPC call
    // so that we can just do something like const response = Amqp.RPC({..details...});
    publish(payload + "!parsed!", replyTo, "DO_NOT_REPLY!", correlation);
  }

  Amqp.listen({
    queue_name: queueName,
    listener: listener,
    auto_ack: true,
    consumer: consumerName,
    // exclusive: false,
    // no_local: false,
    // no_wait: false,
    // args: null
  })

  // const replyListener = function (data) {
  //   console.log(`received response!: ${data}`)
  // }

  // Amqp.listen({
  //   queue_name: replyToQueue,
  //   listener: replyListener,
  //   auto_ack: true,
  //   consumer: "reply to consumer",
  //   // exclusive: false,
  //   // no_local: false,
  //   // no_wait: false,
  //   // args: null
  // })

}

<?php
namespace mle86\WQ\WorkServerAdapter;

use mle86\WQ\Exception\UnserializationException;
use mle86\WQ\Job\Job;
use mle86\WQ\Job\QueueEntry;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * This adapter class implements the {@see WorkServerAdapter} interface.
 *
 * It connects to an AMQP server such as RabbitMQ.
 *
 * It creates durable queues in the default exchange (“”) for immediate-delivery jobs
 * and the durable “`_phpwq._delayed`” queue in the custom “`_phpwq._delay_exchange`” exchange for delayed jobs.
 * Jobs stored with {@see storeJob()} are always durable as well.
 * Empty queues or exchanges won't be deleted automatically.
 *
 * @see https://github.com/php-amqplib/php-amqplib  Uses the php-amqplib/php-amqplib package
 * @see https://www.rabbitmq.com/  RabbitMQ homepage
 */
class AMQPWorkServer
    implements WorkServerAdapter
{

    /** @var AMQPStreamConnection */
    private $connection;
    /** @var AMQPChannel */
    private $chan;
    /** @var string */
    private $exchange = '';  // AMQP default
    /** @var array  [queueName => queueConsumerTag, …] */
    private $current_queues = [];
    /** @var string */
    private $consumer_tag;
    /** @var AMQPMessage|null */
    private $last_msg;
    /** @var string|null */
    private $last_queue;

    /**
     * Constructor.
     * Takes an already-configured {@see AMQPStreamConnection} instance to work with.
     * Does not attempt to establish a connection itself --
     * use the {@see connect()} factory method for that instead.
     *
     * @param AMQPStreamConnection $connection
     */
    public function __construct(AMQPStreamConnection $connection)
    {
        $this->connection = $connection;
        $this->chan = $this->connection->channel();
        $this->consumer_tag = uniqid('_phpwq._ct-', true);
    }

    /**
     * Factory method.
     * This will create a new {@see AMQPStreamConnection} instance by itself.
     *
     * See {@see AMQPStreamConnection::__construct} for the parameter descriptions --
     * the parameters are the same, although this class has more default settings.
     */
    public static function connect(
        $host = 'localhost',
        $port = 5672,
        $user = 'guest',
        $password = 'guest',
        $vhost = '/',
        $insist = false,
        $login_method = 'AMQPLAIN',
        $login_response = null,
        $locale = 'en_US',
        $connection_timeout = 3.0,
        $read_write_timeout = 3.0,
        $context = null,
        $keepalive = false,
        $heartbeat = 0
    ): self {
        $conn = new AMQPStreamConnection(
            $host, $port, $user, $password,
            $vhost, $insist, $login_method, $login_response,
            $locale, $connection_timeout, $read_write_timeout, $context,
            $keepalive, $heartbeat);
        return new self($conn);
    }


    public function getNextQueueEntry($workQueue, int $timeout = self::DEFAULT_TIMEOUT): ?QueueEntry
    {
        if ($timeout === WorkServerAdapter::NOBLOCK) {
            // basic_consume+wait does not support a timeout less than 1sec which is pretty long,
            // so we'll use basic_get() instead:
            return $this->getNextNonblockingQueueEntry((array)$workQueue);
        }

        if ($timeout === WorkServerAdapter::FOREVER) {
            $timeout = 0;
        }

        $this->last_msg   = null;
        $this->last_queue = null;
        $this->consumeQueues((array)$workQueue);

        try {
            $this->chan->wait(null, false, $timeout);
        } catch (AMQPTimeoutException $e) {
            // wait() hit the timeout
            return null;
        }

        if ($this->last_msg === null) {
            // ?!
            return null;
        }

        try {
            // We got a message in $last_msg, try to decode and return it:
            return QueueEntry::fromSerializedJob(
                $this->last_msg->getBody(),
                $this->last_queue,
                $this->last_msg,
                '');

        } catch (UnserializationException $e) {
            $this->buryMessage($this->last_msg);
            throw $e;
        }
    }

    private function getNextNonblockingQueueEntry(array $workQueues): ?QueueEntry
    {
        foreach ($workQueues as $workQueue) {
            $this->initQueue($workQueue);
            /** @var AMQPMessage|null $msg */
            if (($msg = $this->chan->basic_get($workQueue, false))) {
                return QueueEntry::fromSerializedJob(
                    $msg->getBody(),
                    $workQueue,
                    $msg,
                    '');
            }
        }

        // No messages were available right now
        return null;
    }


    public function storeJob(string $workQueue, Job $job, int $delay = 0): void
    {
        $chan = $this->chan;
        $this->initQueue($workQueue);

        $messageProperties = [
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ];

        $targetExchange    = $this->exchange;
        $targetQueue       = $workQueue;

        if ($delay > 0) {
            $messageProperties['expiration'] = (string)($delay * 1000);
            $targetExchange = $this->delayExchange();
        }

        $serializedJob = serialize($job);
        $serializedJobMessage = new AMQPMessage($serializedJob, $messageProperties);

        $chan->basic_publish($serializedJobMessage, $targetExchange, $targetQueue, true);
    }

    /**
     * {@inheritdoc}
     *
     * @param QueueEntry $entry
     */
    public function buryEntry(QueueEntry $entry): void
    {
        $this->buryMessage($entry->getHandle());
    }

    private function buryMessage(AMQPMessage $message): void
    {
        # TODO
    }

    public function requeueEntry(QueueEntry $entry, int $delay, string $workQueue = null): void
    {
        $queue = $workQueue ?? $entry->getWorkQueue();

        $this->deleteEntry($entry);
        $this->storeJob($queue, $entry->getJob(), $delay);
    }

    public function deleteEntry(QueueEntry $entry): void
    {
        $this->deleteMessage($entry->getHandle());
    }

    private function deleteMessage(AMQPMessage $amqpMessage): void
    {
        $deliveryTag = $amqpMessage->delivery_info['delivery_tag'];
        $this->chan->basic_ack($deliveryTag);
    }

    /**
     * Returns the name of the delay exchange to use for delayed messages
     * and declares it once.
     *
     * Delayed messages are not routed through the default exchange (""),
     * they go to our special "_phpwq._delay_exchange" exchange instead.
     * That's a FANOUT exchange bound only to the "_phpwq._delayed" queue
     * and the only reason why the special exchange exists in the first place:
     * so our delayed messages can keep their original routing key unchanged.
     *
     * The special "_phpwq._delayed" queue has a DLX setting set to the default exchange
     * (x-dead-letter-exchange: "")
     * so that expired messages in the queue will be put back into the default exchange.
     * Because neither our special delay exchange nor our special delay queue
     * have a x-dead-letter-routing-key setting,
     * the messages will keep their original routing key
     * and will therefore reach the correct target queue in the default exchange.
     *
     * Our delayed messages carry their delay in their per-message TTL setting (expiration).
     *
     * @return string
     */
    private function delayExchange(): string
    {
        $exchange_name = '_phpwq._delay_exchange';
        $queue_name    = '_phpwq._delayed';

        if (!$this->declared_delay_exchange) {
            $this->declared_delay_exchange = true;

            $args = new AMQPTable([
                // After their TTL expires, messages get re-routed to the correct exchange:
                'x-dead-letter-exchange' => $this->exchange,
                // Without 'x-dead-letter-routing-key', the message's original routing key will be re-used
                // which should ensure that the message finally reaches the correct queue.
            ]);

            $this->chan->exchange_declare($exchange_name, 'fanout', false, true, false);
            $this->chan->queue_declare($queue_name, false, true, false, false, false, $args);
            $this->chan->queue_bind($queue_name, $exchange_name);
        }

        return $exchange_name;
    }

    private $declared_delay_exchange = false;

    private function consumeQueues(array $workQueues): void
    {
        if (array_keys($this->current_queues) === $workQueues) {
            // Same consumption list, so we don't have to change anything.
            // The previous callbacks are also still good because they all write to $last_qe.
            return;
        }

        $chan = $this->chan;

        // first, unregister from all previous queues:
        foreach ($this->current_queues as $wq => $consumerTag) {
            $chan->basic_cancel($consumerTag);
        }

        $this->current_queues = [];

        // now register with all queues to be consumed:
        foreach ($workQueues as $workQueue) {
            $consumerTag = $this->queueConsumerTag($workQueue);
            $this->current_queues[$workQueue] = $consumerTag;

            $callback = function(AMQPMessage $msg) use($workQueue) {
                // Pass info out to the getNextQueueEntry method:
                $this->last_msg   = $msg;
                $this->last_queue = $workQueue;
            };

            $this->initQueue($workQueue);
            $chan->basic_consume($workQueue, $consumerTag, false, false, false, false, $callback);
        }
    }

    private function queueConsumerTag(string $workQueue): string
    {
        return $this->consumer_tag . '.' . $workQueue;
    }

    private function initQueue(string $workQueue): void
    {
        $this->chan->queue_declare($workQueue,
            false, true, false, false);
    }

}

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
    /** @var bool */
    private $declared_delay_exchange = false;
    /** @var bool */
    private $declared_bury_exchange = false;

    private const AMQP_NAMESPACE = '_phpwq.';


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
        $this->consumer_tag = uniqid(self::AMQP_NAMESPACE . '_ct-', true);
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
                $this->getJobId($this->last_msg));

        } catch (UnserializationException $e) {
            $this->buryMessage($this->last_msg, $this->last_queue);
            throw $e;
        }
    }

    /**
     * Returns the next queue entry from any of the queue arguments,
     * immediately returning NULL if they're all empty.
     *
     * {@see AMQPChannel::wait} has a timeout argument,
     * but it cannot be lower than 1 second.
     * That's why {@see getNextQueueEntry} defers to this method
     * when called with <tt>timeout={@see WorkServerAdapter::NOBLOCK}</tt>.
     *
     * To this end, it uses `basic_get()` instead of `basic_consume()`+`wait()`.
     *
     * @param string[] $workQueues
     * @return QueueEntry|null
     */
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
                    $this->getJobId($msg));
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
     * Internally,
     * this re-publishes the failed job to the bury exchange ("_phpwq._bury_exchange")
     * where it will end up in the bury queue ("_phpwq._BURIED").
     *
     * @param QueueEntry $entry
     */
    public function buryEntry(QueueEntry $entry): void
    {
        $this->buryMessage($entry->getHandle(), $entry->getWorkQueue());
    }

    private function buryMessage(AMQPMessage $amqpMessage, string $originWorkQueue): void
    {
        // first get rid of the original message...
        $this->deleteMessage($amqpMessage);

        // then store an exact copy in the Bury exchange/queue for later inspection:
        $targetExchange = $this->buryExchange();
        $this->chan->basic_publish($amqpMessage, $targetExchange, $originWorkQueue, true);
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
        $exchange_name = self::AMQP_NAMESPACE . '_delay_exchange';
        $queue_name    = self::AMQP_NAMESPACE . '_delayed';

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

    /**
     * Returns the name of the bury exchange to use for buried messages
     * and declares it once.
     *
     * Similar to the {@see delayExchange},
     * this is a FANOUT exchange bound to one single durable queue ("_phpwq._BURIED")
     * so that all messages published to that exchange end up in that queue
     * regardless of their actual routing key --
     * this way, their original routing key (i.e. target work queue name)
     * is still available in their delivery_info.
     *
     * @return string
     */
    private function buryExchange(): string
    {
        $exchange_name = self::AMQP_NAMESPACE . '_bury_exchange';
        $queue_name    = self::AMQP_NAMESPACE . '_BURIED';

        if (!$this->declared_bury_exchange) {
            $this->declared_bury_exchange = true;

            $this->chan->exchange_declare($exchange_name, 'fanout', false, true, false);
            $this->chan->queue_declare($queue_name, false, true, false, false, false, $args);
            $this->chan->queue_bind($queue_name, $exchange_name);
        }

        return $exchange_name;
    }

    /**
     * Calls {@see AMQPChannel::basic_consume} on one or more queue names.
     *
     * Internally it keeps track of the list of consumed queue names
     * and only re-issues the `basic_consume` calls if the list has changed.
     * In that case, the previous queue subscriptions are {@see AMQPChannel::basic_cancel}led.
     *
     * @param string[] $workQueues
     */
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

    /**
     * The consumer tag used for one queue name.
     * This is needed so that {@see consumeQueues} is able to later cancel queue subscriptions.
     *
     * @param string $workQueue
     * @return string
     */
    private function queueConsumerTag(string $workQueue): string
    {
        return $this->consumer_tag . '.' . $workQueue;
    }

    /**
     * Declares a queue by name with suitable default settings
     * if it does not exist already.
     *
     * @param string $workQueue
     */
    private function initQueue(string $workQueue): void
    {
        $this->chan->queue_declare($workQueue,
            false, true, false, false);
    }

    /**
     * The {@see QueueEntry} constructor requires a string “job ID” for every instance,
     * although some implementations simply use the empty string
     * because the underlying work server does not offer any usable identifier.
     *
     * @param AMQPMessage $amqpMessage
     * @return string
     */
    private function getJobId(AMQPMessage $amqpMessage): string
    {
        return $amqpMessage->delivery_info['delivery_tag'] ?? '';
    }

}

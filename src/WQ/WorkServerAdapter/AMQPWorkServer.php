<?php

namespace mle86\WQ\WorkServerAdapter;

use mle86\WQ\Exception\UnserializationException;
use mle86\WQ\Job\Job;
use mle86\WQ\Job\QueueEntry;
use mle86\WQ\WorkProcessor;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
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
class AMQPWorkServer implements WorkServerAdapter
{

    /**
     * When hitting a timeout, {@see getNextQueueEntry} will cancel all queue subscriptions
     * so that no messages get delivered to us _before_ the call (if there even is one)
     * as they'd possibly stay reserved for the current process for a long time.
     * There's still a race condition: cancelling the queues after a timeout is not an
     * atomic action. That race condition could be eliminated by always calling
     * {@see AMQPChannel::basic_recover} after the cancellations, but this would require users
     * to delete/bury/requeue all entries before calling getNextQueueEntry again
     * or risk possibly infinite job duplicates. (The {@see WorkProcessor} helper does this correctly.)
     */
    private const RECOVER_AFTER_TIMEOUT = true;


    /** @var AMQPStreamConnection */
    private $connection;
    /** @var AMQPChannel */
    private $chan;
    /** @var string */
    private $exchange = '';  // AMQP default
    /** @var array  [queueName => queueConsumerTag, …] */
    private $currentQueues = [];
    /** @var AMQPMessage|null  The last received amqp message. Set by the message callback set up in {@see consumeQueues()}. */
    private $lastMsg;
    /** @var string|null  The quere from which the last amqp message was received. Set by the message callback set up in {@see consumeQueues()}. */
    private $lastQueue;
    /** @var bool */
    private $isDelayExchangeDeclared = false;
    /** @var bool */
    private $isBuryExchangeDeclared = false;

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
        $this->checkConnection();

        if ($timeout === WorkServerAdapter::NOBLOCK) {
            // basic_consume+wait does not support a timeout less than 1sec which is pretty long,
            // so we'll use basic_get() instead:
            return $this->getNextNonblockingQueueEntry((array)$workQueue);
        }

        if ($timeout === WorkServerAdapter::FOREVER) {
            $timeout = 0;
        }

        $this->lastMsg   = null;
        $this->lastQueue = null;
        $this->consumeQueues((array)$workQueue);

        try {
            $this->chan->wait(null, false, $timeout);
        } catch (AMQPTimeoutException $e) {
            // wait() hit the timeout
            $this->consumeQueues([]);
            return null;
        }

        if ($this->lastMsg === null) {
            // ?!
            // This means that wait() has just processed a deferred message
            // for a previously-subscribed queue but we had cancelled the subscription
            // before actually handling the message. That means our callback was already gone.
            // The message is not lost: consumeQueues() has called basic_recover() already.
            // Still, this is not the result we want to return to our callers.
            // Let's try once more:
            $this->chan->wait(null, true, null);
            if ($this->lastMsg === null) {
                // Ok, we give up for now
                return null;
            }
        }

        try {
            // We got a message in $last_msg, try to decode and return it:
            return QueueEntry::fromSerializedJob(
                $this->lastMsg->getBody(),
                $this->lastQueue,
                $this->lastMsg,
                $this->getJobId($this->lastMsg));

        } catch (UnserializationException $e) {
            $this->buryMessage($this->lastMsg, $this->lastQueue);
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
        $this->checkConnection();
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
        $this->checkConnection();
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
        $exchangeName = self::AMQP_NAMESPACE . '_delay_exchange';
        $queueName    = self::AMQP_NAMESPACE . '_delayed';

        if (!$this->isDelayExchangeDeclared) {
            $this->isDelayExchangeDeclared = true;

            $args = new AMQPTable([
                // After their TTL expires, messages get re-routed to the correct exchange:
                'x-dead-letter-exchange' => $this->exchange,
                // Without 'x-dead-letter-routing-key', the message's original routing key will be re-used
                // which should ensure that the message finally reaches the correct queue.
            ]);

            $this->chan->exchange_declare($exchangeName, 'fanout', false, true, false);
            $this->chan->queue_declare($queueName, false, true, false, false, false, $args);
            $this->chan->queue_bind($queueName, $exchangeName);
        }

        return $exchangeName;
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
        $exchangeName = self::AMQP_NAMESPACE . '_bury_exchange';
        $queueName    = self::AMQP_NAMESPACE . '_BURIED';

        if (!$this->isBuryExchangeDeclared) {
            $this->isBuryExchangeDeclared = true;

            $this->chan->exchange_declare($exchangeName, 'fanout', false, true, false);
            $this->chan->queue_declare($queueName, false, true, false, false, false);
            $this->chan->queue_bind($queueName, $exchangeName);
        }

        return $exchangeName;
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
        if (array_keys($this->currentQueues) === $workQueues) {
            // Same consumption list, so we don't have to change anything.
            // The previous callbacks are also still good because they all write to $lastMsg.
            return;
        }

        $chan = $this->chan;

        // first, unregister from all previous queues:
        foreach ($this->currentQueues as $wq => $consumerTag) {
            $chan->basic_cancel($consumerTag);
        }

        // Now release all previously-received, unACK'ed messages:
        if ($this->currentQueues) {
            if ($workQueues || self::RECOVER_AFTER_TIMEOUT) {
                $this->chan->basic_recover(true);
            } else {
                // empty(workQueues) only happens when getNextQueueEntry() hits a timeout.
                // That case usually doesn't require a recovery.
            }
        }

        $this->currentQueues = [];  // !

        $myLastMsg   =& $this->lastMsg;
        $myLastQueue =& $this->lastQueue;

        // now register with all queues to be consumed:
        foreach ($workQueues as $workQueue) {
            $callback = static function(AMQPMessage $msg) use($workQueue, &$myLastMsg, &$myLastQueue) {
                // Pass info out to the getNextQueueEntry method:
                $myLastMsg   = $msg;
                $myLastQueue = $workQueue;

                /* Usually we'd simply write to $this->lastMsg and $this->lastQueue here,
                 * but that means that the closure contains a reference to $this AMQPWorkServer.
                 * We store the AMQPChannel in $this->chan but that object also stores the closures somewhere
                 * (until the subscription gets cancelled again),
                 * so we have a circular reference that may persist for quite some time
                 * and cause a memory leak (at least until the next gc_collect_cycles call).
                 * Using "static function" avoids that (the closure has no $this reference anymore).  */
            };

            $this->initQueue($workQueue);
            $consumerTag = $chan->basic_consume(
                $workQueue,
                '',
                false,
                false,
                false,
                false,
                $callback);
            $this->currentQueues[$workQueue] = $consumerTag;
        }
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
        return $amqpMessage->getDeliveryTag();
    }

    private function checkConnection(): void
    {
        if (!$this->connection->isConnected()) {
            $this->connection->reconnect();  // !
            $this->chan = $this->connection->channel();
            $this->currentQueues = [];
        }
    }

    public function __destruct()
    {
        // The connection object may still be reachable through some hidden circular reference.
        // Make sure there's no real connection anymore:
        $this->disconnect();
    }

    public function disconnect(): void
    {
        if ($this->connection) {
            $this->connection->close();
            $this->chan = null;
            $this->connection = null;
        }
    }
}

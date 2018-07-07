<?php
namespace mle86\WQ\WorkServerAdapter;

use mle86\WQ\Exception\UnserializationException;
use mle86\WQ\Job\Job;
use mle86\WQ\Job\QueueEntry;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * This adapter class implements the {@see WorkServerAdapter} interface.
 *
 * It connects to an AMQP server such as RabbitMQ.
 *
 * @see https://github.com/php-amqplib/php-amqplib  Uses the php-amqplib/php-amqplib package
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
    /** @var array  [queueName => true, …]  List of queues currently opened via initQueue() and not yet closed. */
    private $open_queues = [];
    /** @var string */
    private $consumer_tag;
    /** @var AMQPMessage|null */
    private $last_msg;
    /** @var string|null */
    private $last_queue;
    /** @var bool */
    private $do_cleanup = true;
    /** @var array  [workQueue => [deliveryTag => true, …]]  Delivery tags of messages that have been received but not ACK'ed by us. */
    private $open_messages = [];

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
        $this->consumer_tag = uniqid('WQ.', true);

        // On shutdown we'd like to delete all unused, empty queues we created previously.
        // We cannot do this in __destruct because at this point the channel connection is probably gone,
        // so it'll have to go in a shutdown handler :(
        register_shutdown_function(function(){
            if ($this->do_cleanup) {
                $this->deleteAllKnownEmptyQueues();
            }
        });
    }

    /**
     * Sets the cleanup flag for this instance.
     *
     * If it's true (default),
     * the instance installs a shutdown handler
     * that tries to delete all queues it previously touched
     * (only if they're really empty and not currently in use).
     *
     * @param bool $do_cleanup
     * @return self
     */
    public function withCleanup(bool $do_cleanup = true): self
    {
        $this->do_cleanup = $do_cleanup;
        return $this;
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


    /**
     * This takes the next job from the named work queue
     * and returns it.
     *
     * @param string|string[] $workQueue The name(s) of the Work Queue(s) to poll.
     * @param int $timeout               How many seconds to wait for a job to arrive, if none is available immediately.
     *                                   Set this to NOBLOCK if the method should return immediately.
     *                                   Set this to BLOCK if the call should block until a job becomes available, no matter how long it takes.
     * @return QueueEntry  Returns the next job in the work queue,
     *                                   or NULL if no job was available after waiting for $timeout seconds.
     * @throws UnserializationException
     */
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
            $qe = QueueEntry::fromSerializedJob(
                $this->last_msg->getBody(),
                $this->last_queue,
                $this->last_msg,
                '');

            $this->open_messages[$this->last_queue][$this->last_msg->delivery_info['delivery_tag']] = true;
            return $qe;

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
                $qe = QueueEntry::fromSerializedJob(
                    $msg->getBody(),
                    $workQueue,
                    $msg,
                    '');

                $this->open_messages[$workQueue][$msg->delivery_info['delivery_tag']] = true;
                return $qe;
            }
        }

        // No messages were available right now
        return null;
    }


    /**
     * Stores a job in the work queue for later processing.
     *
     * @param string $workQueue The name of the Work Queue to store the job in.
     * @param Job $job          The job to store.
     * @param int $delay        The job delay in seconds after which it will become available to {@see getNextQueueEntry()}.
     *                          Set to zero (default) for jobs which should be processed as soon as possible.
     */
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
     * Buries an existing job
     * so that it won't be returned by {@see getNextQueueEntry()} again
     * but is still present in the system for manual inspection.
     *
     * This is what happens to failed jobs.
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

    /**
     * Re-queues an existing job
     * so that it can be returned by {@see getNextQueueEntry()}
     * again at some later time.
     * A {@see $delay} is required
     * to prevent the job from being returned right after it was re-queued.
     *
     * This is what happens to failed jobs which can still be re-queued for a retry.
     *
     * @param QueueEntry $entry       The job to re-queue. The instance should not be used anymore after this call.
     * @param int $delay              The job delay in seconds. It will become available for {@see getNextQueueEntry()} after this delay.
     * @param string|null $workQueue  By default, to job is re-queued into its original Work Queue ({@see QueueEntry::getWorkQueue}).
     *                                With this parameter, a different Work Queue can be chosen.
     */
    public function requeueEntry(QueueEntry $entry, int $delay, string $workQueue = null): void
    {
        $queue = $workQueue ?? $entry->getWorkQueue();

        $this->deleteEntry($entry);
        $this->storeJob($queue, $entry->getJob(), $delay);
    }

    /**
     * Permanently deletes a job entry for its work queue.
     *
     * This is what happens to finished jobs.
     *
     * @param QueueEntry $entry The job to delete.
     */
    public function deleteEntry(QueueEntry $entry): void
    {
        /** @var AMQPMessage $amqpMessage */
        $amqpMessage = $entry->getHandle();
        $deliveryTag = $amqpMessage->delivery_info['delivery_tag'];
        $this->chan->basic_ack($deliveryTag);
        unset($this->open_messages[$entry->getWorkQueue()][$deliveryTag]);
    }

    /**
     * Returns the name of the delay exchange to use for delayed messages
     * and declares it once.
     *
     * @return string
     */
    private function delayExchange(): string
    {
        $ex = '_wq_delay_dlx';

        if (!$this->declared_delay_exchange) {
            $this->declared_delay_exchange = true;

            $chan = $this->chan;

            $args = [
                // After their TTL expires, messages get re-routed to the original exchange:
                'x-dead-letter-exchange' => $this->exchange,
                // Without 'x-dead-letter-routing-key', the message's original routing key will be re-used
                // which should ensure that the message finally reaches the correct queue.
            ];
            $chan->exchange_declare($ex, 'direct', false, true, true, false, false, $args);
        }

        return $ex;
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
            $this->deleteEmptyQueue($wq);
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
        return $this->consumer_tag . '-' . $workQueue;
    }

    private function initQueue(string $workQueue): void
    {
        $this->chan->queue_declare($workQueue,
            false, true, false, false);
        $this->open_queues[$workQueue] = true;
    }

    private function deleteEmptyQueue(string $workQueue): void
    {
        if (!empty($this->open_messages[$workQueue])) {
            // Cannot delete wq, it has open messages.
            // queue_delete(if_empty) would STILL delete that wq because THIS client cannot see the messages anymore!
            return;
        }

        try {
            unset($this->open_queues[$workQueue]);
            $this->chan->queue_delete($workQueue, true, true);
        } catch (AMQPProtocolChannelException $e) {
            // don't care
            return;
        }
    }

    private function deleteAllKnownEmptyQueues(): void
    {
        if (!$this->chan || !$this->chan->getConnection() || !$this->chan->getConnection()->isConnected()) {
            return;
        }

        foreach ($this->current_queues as $wq => $consumerTag) {
            // cancel all active subscriptions or deleteEmptyQueue() won't work
            // as it only deletes queues that aren't in use:
            $this->chan->basic_cancel($consumerTag);
            $this->deleteEmptyQueue($wq);
        }
        foreach (array_keys($this->open_queues) as $workQueue) {
            // now delete all other queues we touched (with timeout=NOBLOCK):
            $this->deleteEmptyQueue($workQueue);
        }
    }

}

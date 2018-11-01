<?php
namespace mle86\WQ\Tests;

use mle86\WQ\Testing\AbstractWorkServerAdapterTest;
use mle86\WQ\WorkServerAdapter\WorkServerAdapter;
use mle86\WQ\WorkServerAdapter\AMQPWorkServer;
use PhpAmqpLib\Connection\AMQPStreamConnection;

class AMQPServerTest
    extends AbstractWorkServerAdapterTest
{

    const DEFAULT_PORT = 5672;

    public function checkEnvironment(): void
    {
        $this->checkInDocker();

        $this->assertNotFalse(getenv('RABBITMQ_NODE_PORT'),
            "No RABBITMQ_NODE_PORT ENV variable found! Is this test running in the test container?");
        $this->assertGreaterThan(1024, getenv('RABBITMQ_NODE_PORT'),
            "Invalid RABBITMQ_NODE_PORT ENV variable!");
        $this->assertNotEquals(self::DEFAULT_PORT, getenv('RABBITMQ_NODE_PORT'),
            "RABBITMQ_NODE_PORT ENV variable should NOT be set to the default AMQP port! " .
            "This prevents the test scripts from accidentally running on the host system.");

        $this->assertNoDefaultConnection('localhost', self::DEFAULT_PORT);
    }

    private function assertNoDefaultConnection(string $host, int $port): void
    {
        $e = null;
        try {
            (new AMQPWorkServer (new AMQPStreamConnection($host, $port, 'guest', 'guest')))
                ->getNextQueueEntry("@this-should-not-exist-29743984375345", AMQPWorkServer::NOBLOCK);
        } catch (\Exception $e) {
            // continue...
        }

        if ($e instanceof \ErrorException && strpos($e->getMessage(), 'unable to connect') !== false) {
            // ok!
            return;
        }

        throw new \RuntimeException(
            "We managed to get an AMQP stream connection on the AMQP default port! " .
            "This should not be possible inside the test container.",
            0, $e);
    }

    public function getWorkServerAdapter(): WorkServerAdapter
    {
        return AMQPWorkServer::connect("localhost", (int)getenv('RABBITMQ_NODE_PORT'));
    }

}

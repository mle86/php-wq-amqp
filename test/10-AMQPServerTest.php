<?php
namespace mle86\WQ\Tests;

use mle86\WQ\WorkServerAdapter\WorkServerAdapter;
use mle86\WQ\WorkServerAdapter\AMQPWorkServer;
use PhpAmqpLib\Connection\AMQPStreamConnection;

require_once __DIR__ . '/../vendor/mle86/wq/test/helper/AbstractWorkServerAdapterTest.php';

class AMQPServerTest
    extends AbstractWorkServerAdapterTest
{

    const DEFAULT_PORT = 5672;

    public function checkEnvironment(): void
    {
        $this->assertNotFalse(getenv('RABBITMQ_NODE_PORT'),
            "No RABBITMQ_NODE_PORT ENV variable found! Is this test running in the test container?");
        $this->assertGreaterThan(1024, getenv('RABBITMQ_NODE_PORT'),
            "Invalid RABBITMQ_NODE_PORT ENV variable!");
    }

    public function getWorkServerAdapter(): WorkServerAdapter
    {
        return AMQPWorkServer::connect("localhost", (int)getenv('RABBITMQ_NODE_PORT'));
    }

}

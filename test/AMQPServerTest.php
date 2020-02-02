<?php

namespace mle86\WQ\Tests;

use mle86\WQ\Testing\AbstractWorkServerAdapterTest;
use mle86\WQ\WorkServerAdapter\WorkServerAdapter;
use mle86\WQ\WorkServerAdapter\AMQPWorkServer;
use PhpAmqpLib\Connection\AMQPStreamConnection;

class AMQPServerTest extends AbstractWorkServerAdapterTest
{

    public function checkEnvironment(): void
    {
        $this->checkInDocker();

        $this->assertEquals(1, getenv('IS_MLE86_WQ_AMQP_TEST'),
            "Container env var IS_MLE86_WQ_AMQP_TEST not set -- is this the correct test container?");
    }

    public function getWorkServerAdapter(): WorkServerAdapter
    {
        return AMQPWorkServer::connect("localhost");
    }

}

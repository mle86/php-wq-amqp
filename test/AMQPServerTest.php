<?php

namespace mle86\WQ\Tests;

use mle86\WQ\Testing\AbstractWorkServerAdapterTest;
use mle86\WQ\Testing\SimpleTestJob;
use mle86\WQ\WorkServerAdapter\WorkServerAdapter;
use mle86\WQ\WorkServerAdapter\AMQPWorkServer;
use PhpAmqpLib\Connection\AMQPStreamConnection;

class AMQPServerTest extends AbstractWorkServerAdapterTest
{

    public function checkEnvironment(): void
    {
        $this->checkInDockerOrTravis();

        $this->assertEquals(1, getenv('IS_MLE86_WQ_AMQP_TEST'),
            "Container env var IS_MLE86_WQ_AMQP_TEST not set -- is this the correct test container?");
    }

    public function getWorkServerAdapter(): WorkServerAdapter
    {
        return AMQPWorkServer::connect("localhost");
    }


    public function additionalTests(WorkServerAdapter $ws): void
    {
        $this->checkTimeoutBug($ws);
    }

    private function checkTimeoutBug(WorkServerAdapter $ws): void
    {
        $this->assertNull($ws->getNextQueueEntry(["QB"], 1));

        $ws->storeJob("QA",  new SimpleTestJob(611));
        $ws->storeJob("QB",  new SimpleTestJob(622));
        $qa1 = $ws->getNextQueueEntry("QA", 1);
        $qa2 = $ws->getNextQueueEntry("QA", 1);
        $qb1 = $ws->getNextQueueEntry(["QB","QC"], 1);
        $qb2 = $ws->getNextQueueEntry(["QB","QC"], 1);
        $ws->storeJob("QC",  new SimpleTestJob(633));
        $qb3 = $ws->getNextQueueEntry(["QB","QC"], 1);
        $qb4 = $ws->getNextQueueEntry(["QB","QC"], 1);

        $expectedSequence = [611, 0,  622, 0,  633, 0];
        $receivedSequence = [
            ($qa1) ? $qa1->getJob()->getMarker() : 0,
            ($qa2) ? $qa2->getJob()->getMarker() : 0,
            ($qb1) ? $qb1->getJob()->getMarker() : 0,
            ($qb2) ? $qb2->getJob()->getMarker() : 0,
            ($qb3) ? $qb3->getJob()->getMarker() : 0,
            ($qb4) ? $qb4->getJob()->getMarker() : 0,
        ];

        $this->assertSame($expectedSequence, $receivedSequence,
            "After one getNextQueueEntry() timeout, subsequent messages were not received correctly!");
    }

}

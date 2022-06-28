<?php

namespace mle86\WQ\Exception;

use PhpAmqpLib\Exception\AMQPRuntimeException;

class AMQPConnectionException extends \RuntimeException implements WQConnectionException
{

    public static function wrap(AMQPRuntimeException $e): self
    {
        return new self($e->getMessage(), 0, $e);
    }

}

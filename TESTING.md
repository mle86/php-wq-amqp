# Testing

As usual for `WorkServerAdapter` implementations,
[our test class](test/10-AMQPServerTest.php)
extends from
[WQ's `AbstractWorkServerAdapterTest`](https://github.com/mle86/php-wq/blob/master/test/helper/AbstractWorkServerAdapterTest.php),
which performs a series of standardized tests
on our implementation.

Those tests run on an actual RabbitMQ instance.
To keep things isolated from your system,
they are run inside a [custom Docker container](Dockerfile)
based on the [php:7.1-cli](https://hub.docker.com/_/php/) image.

To run all tests,
run
`$ make test`.

This will automatically build the `mle86/php-wq-amqp-test` image
(run `$ make test-image` to do so manually)
and delete the container afterwards.

If you want to remove the Docker image afterwards,
run
`$ make clean`.


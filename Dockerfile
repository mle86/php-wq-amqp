# see TESTING.md

FROM php:7.1-cli

RUN \
	apt-get update  && \
	apt-get install -y --no-install-recommends  rabbitmq-server  && \
	docker-php-ext-configure bcmath --enable-bcmath  && \
	docker-php-ext-configure sockets  && \
	docker-php-ext-install sockets bcmath

VOLUME ["/mnt"]

# The variable affects both 'rabbitmq-server' and the 10-AMQPServerTest.php script.
# The uncommon value should prevent the tests from accidentally running on a host with a real RabbitMQ instance.
ENV RABBITMQ_NODE_PORT 35672

ADD docker-start.sh /start.sh
RUN chmod +x /start.sh
ENTRYPOINT ["/start.sh"]

WORKDIR /mnt
CMD ["vendor/bin/phpunit"]

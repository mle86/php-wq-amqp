# see TESTING.md

FROM php:7.1-cli

RUN \
	apt-get update  && \
	apt-get install -y --no-install-recommends  rabbitmq-server  && \
	docker-php-ext-configure bcmath --enable-bcmath  && \
	docker-php-ext-configure sockets  && \
	docker-php-ext-install sockets bcmath

RUN \
	pecl install xdebug  && \
	docker-php-ext-enable xdebug

VOLUME ["/mnt"]

# Changing the rabbitmq port with RABBITMQ_NODE_PORT works fine for the rabbitmq-server itself,
# but rabbitmqctl does not seem to notice the variable.
# This leads to incorrect error messages on start-up and too little delay.
# So now we're running the server on the default port.
# To avoid some of the inherent risks, our tests will make sure they're running in the correct environment:
ENV IS_MLE86_WQ_AMQP_TEST=1

ADD docker-start.sh /start.sh
RUN chmod +x /start.sh
ENTRYPOINT ["/start.sh"]

WORKDIR /mnt
CMD ["vendor/bin/phpunit"]

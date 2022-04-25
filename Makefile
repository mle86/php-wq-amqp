#!/usr/bin/make -f
.PHONY: all dep update test-image test clean

all:

COMPOSER=./composer.phar

TEST_IMAGE=mle86/php-wq-amqp-test
TEST_IMAGE_VERSION=1.1.0


# dep: Install dependencies necessary for development work on this library.
dep: $(COMPOSER)
	[ -d vendor/ ] || $(COMPOSER) install

# composer.phar: Get composer binary from authoritative source
$(COMPOSER):
	curl -sS https://getcomposer.org/installer | php

# update: Updates all composer dependencies of this library.
update: $(COMPOSER)
	$(COMPOSER) update

# see TESTING.md
test-image:
	[ -n "`docker images -q '$(TEST_IMAGE):$(TEST_IMAGE_VERSION)'`" ] \
	|| docker build --tag $(TEST_IMAGE):$(TEST_IMAGE_VERSION) .

# test: Executes all phpUnit tests according to the local phpunit.xml.dist file.
test: dep test-image
	chmod 0777 build/logs/
	docker run --rm \
		--volume "`pwd`":/mnt:ro  \
		--volume "`pwd`/build/logs":/mnt/build/logs:rw  \
		$(TEST_IMAGE):$(TEST_IMAGE_VERSION)  \
		vendor/bin/phpunit -v

clean:
	docker rmi $(TEST_IMAGE):$(TEST_IMAGE_VERSION)  || true


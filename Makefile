#!/usr/bin/make -f
.PHONY: all dep update

all:

COMPOSER=./composer.phar


# dep: Install dependencies necessary for development work on this library.
dep: $(COMPOSER)
	[ -d vendor/ ] || $(COMPOSER) install

# composer.phar: Get composer binary from authoritative source
$(COMPOSER):
	curl -sS https://getcomposer.org/installer | php

# update: Updates all composer dependencies of this library.
update: $(COMPOSER)
	$(COMPOSER) update


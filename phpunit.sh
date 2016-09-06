#!/usr/bin/env bash

# fix for travis
export IMPALA_DB_PASSWORD="";

composer selfupdate
composer install -n

waitforservices

./vendor/bin/phpcs --standard=psr2 -n --ignore=vendor --extensions=php .
./vendor/bin/phpunit "$@"
./vendor/bin/test-reporter

# Cloudera Impala DB Writer

[![Docker Repository on Quay](https://quay.io/repository/keboola/db-writer-impala/status "Docker Repository on Quay")](https://quay.io/repository/keboola/db-writer-impala)
[![Build Status](https://travis-ci.org/keboola/db-writer-impala.svg?branch=master)](https://travis-ci.org/keboola/db-writer-impala)
[![Code Climate](https://codeclimate.com/github/keboola/db-writer-impala/badges/gpa.svg)](https://codeclimate.com/github/keboola/db-writer-impala)
[![Test Coverage](https://codeclimate.com/github/keboola/db-writer-impala/badges/coverage.svg)](https://codeclimate.com/github/keboola/db-writer-impala/coverage)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/keboola/db-writer-impala/blob/master/LICENSE.md)

Writes data to Cloudera Impala Database.

## Example configuration

```json
    {
      "db": {
        "host": "HOST",
        "port": "PORT",
        "database": "DATABASE",
        "user": "USERNAME",
        "password": "PASSWORD",
        "auth_mech": 3,
        "ssh": {
          "enabled": true,
          "keys": {
            "private": "ENCRYPTED_PRIVATE_SSH_KEY",
            "public": "PUBLIC_SSH_KEY"
          },
          "sshHost": "PROXY_HOSTNAME"
        }
      },
      "tables": [
        {
          "tableId": "simple",
          "dbName": "dbo.simple",
          "export": true, 
          "incremental": true,
          "primaryKey": ["id"],
          "items": [
            {
              "name": "id",
              "dbName": "id",
              "type": "int",
              "size": null,
              "nullable": null,
              "default": null
            },
            {
              "name": "name",
              "dbName": "name",
              "type": "nvarchar",
              "size": 255,
              "nullable": null,
              "default": null
            },
            {
              "name": "glasses",
              "dbName": "glasses",
              "type": "nvarchar",
              "size": 255,
              "nullable": null,
              "default": null
            }
          ]                                
        }
      ]
    }
```

## Development

App is developed on localhost using TDD.

1. Clone from repository: `git clone git@github.com:keboola/db-writer-impala.git`
2. Change directory: `cd db-writer-impala`
3. Install dependencies: `composer install --no-interaction`
4. Run docker-compose, which will trigger phpunit: `docker-compose run app`

## License

MIT licensed, see [LICENSE](./LICENSE) file.

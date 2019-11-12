<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;

class Impala extends Writer implements WriterInterface
{
    /** @var array $allowedTypes */
    private static $allowedTypes = [
        'bigint', 'boolean', 'char', 'decimal',
        'double', 'float', 'int', 'real',
        'smallint', 'string', 'timestamp',
        'tinyint', 'varchar',
    ];

    /** @var array $typesWithSize */
    private static $typesWithSize = [
        'char',
        'decimal',
        'varchar',
    ];

    /** @var \PDO $db */
    protected $db;

    /** @var Logger $logger */
    protected $logger;

    public function __construct(array $dbParams, Logger $logger)
    {
        parent::__construct($dbParams, $logger);
        $this->logger = $logger;
    }

    public function createConnection(array $dbParams): \PDO
    {
        if (array_key_exists('password', $dbParams) && !array_key_exists('#password', $dbParams)) {
            $dbParams['#password'] = $dbParams['password'];
        }

        // check params
        foreach (['host', 'database', 'user', '#password'] as $param) {
            if (!array_key_exists($param, $dbParams)) {
                throw new UserException(sprintf('Parameter %s is missing.', $param));
            }
        }

        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
        ];

        if (isset($dbParams['auth_mech']) && $dbParams['auth_mech'] === 0) {
            $dbParams['#password'] = '';
        }

        $port = isset($dbParams['port']) ? $dbParams['port'] : '21050';
        $dsn = sprintf(
            'odbc:DSN=%s;HOST=%s;PORT=%s;Database=%s;UID=%s;PWD=%s;AuthMech=%s',
            'MyImpala',
            $dbParams['host'],
            $port,
            $dbParams['database'],
            $dbParams['user'],
            $dbParams['#password'],
            isset($dbParams['auth_mech']) ? $dbParams['auth_mech'] : 3
        );

        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['#password'], $options);
        $pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);

        return $pdo;
    }

    public static function getAllowedTypes(): array
    {
        return self::$allowedTypes;
    }

    public function drop(string $tableName): void
    {
        $this->execQuery(sprintf('DROP TABLE IF EXISTS %s', $this->escape($tableName)));
    }

    public function create(array $table): void
    {
        $sql = "CREATE TABLE `{$table['dbName']}` (";

        $columns = $table['items'];
        foreach ($columns as $k => $col) {
            $type = strtolower($col['type']);
            if ($type === 'ignore') {
                continue;
            }

            if (!empty($col['size']) && in_array($type, self::$typesWithSize)) {
                $type .= "({$col['size']})";
            }

            $sql .= "`{$col['dbName']}` $type";
            $sql .= ',';
        }

        $sql = substr($sql, 0, -1);
        $sql .= ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY '\\\\'";

        $this->execQuery($sql);
    }

    public function write(CsvFile $csv, array $table): void
    {
        $header = $csv->getHeader();
        $headerWithoutIgnored = array_filter($header, function ($column) use ($table) {
            // skip ignored
            foreach ($table['items'] as $tableColumn) {
                if ($tableColumn['name'] === $column && strtolower($tableColumn['type']) === 'ignore') {
                    return false;
                }
            }

            return true;
        });

        $columns = array_filter($table['items'], function ($item) {
            return strtolower($item['type']) !== 'ignore';
        });

        $columnNames = array_map(function ($item) {
            return $this->escape($item['dbName']);
        }, $columns);

        $csv->next();

        $columnsCount = count($csv->current());
        $rowsPerInsert = intval((3000 / $columnsCount) - 1);

        while ($csv->current() !== false) {
            $sql = sprintf(
                'INSERT INTO %s (%s) VALUES',
                $this->escape($table['dbName']),
                implode(',', $columnNames)
            ) . PHP_EOL;

            for ($i=0; $i<$rowsPerInsert && $csv->current() !== false; $i++) {
                $sql .= sprintf(
                    '(%s),',
                    $this->getValuesClause(
                        (array) array_combine($header, $csv->current()),
                        $columns
                    )
                );
                $csv->next();
            }
            // ditch the last coma
            $sql = substr($sql, 0, -1);

            var_dump($sql);
            $this->execQuery($sql);
        }
    }

    public function upsert(array $table, string $targetTable): void
    {
        throw new ApplicationException('Incremental write is not supported.');
    }

    public function tableExists(string $tableName): bool
    {
        $tableArr = explode('.', $tableName);
        $tableName = isset($tableArr[1])?$tableArr[1]:$tableArr[0];

        try {
            $stmt = $this->db->query(sprintf('SELECT * FROM %s LIMIT 1', $this->escape($tableName)));
        } catch (\PDOException $e) {
            return false;
        }

        return true;
    }

    private function execQuery(string $query): void
    {
        $this->logger->info(sprintf("Executing query '%s'", $query));
        $this->db->exec($query);
    }

    public function showTables(string $dbName): array
    {
        // TODO: Implement showTables() method.
    }

    public function getTableInfo(string $tableName): array
    {
        // TODO: Implement getTableInfo() method.
    }

    public function validateTable(array $tableConfig): void
    {
        // TODO: Implement validateTable() method.
    }

    public function generateTmpName(string $tableName): string
    {
        // TODO: Implement generateTmpName() method.
    }

    public function testConnection(): void
    {
        $this->db->prepare('SHOW TABLES')->execute();
    }

    private function escape(string $str): string
    {
        return sprintf('`%s`', $str);
    }

    private function escapeSingleQuotes(string $str): ?string
    {
        return preg_replace("/\'/", "\\'", $str);
    }

    private function getValuesClause(array $row, array $columns): string
    {
        $res = [];
        foreach ($row as $key => $value) {
            foreach ($columns as $column) {
                if ($column['name'] === $key && strtolower($column['type']) !== 'ignore') {
                    if (is_numeric($value)) {
                        $res[] = $value;
                        continue;
                    }

                    $type = $column['type'];
                    if (!is_null($column['size'])) {
                        $type .= sprintf('(%s)', $column['size']);
                    }

                    $res[] = sprintf("cast('%s' as %s)", $this->escapeSingleQuotes($value), $type);
                }
            }
        }

        return implode(',', $res);
    }
}

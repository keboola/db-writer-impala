<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 12/02/16
 * Time: 16:38
 */

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;

class Impala extends Writer implements WriterInterface
{
    private static $allowedTypes = [
        'bigint', 'boolean', 'char', 'decimal',
        'double', 'float', 'int', 'real',
        'smallint', 'string', 'timestamp',
        'tinyint', 'varchar'
    ];

    private static $typesWithSize = [
        'char',
        'decimal',
        'varchar'
    ];

    /** @var \PDO */
    protected $db;

    /** @var Logger */
    protected $logger;

    public function __construct($dbParams, Logger $logger)
    {
        parent::__construct($dbParams, $logger);
        $this->logger = $logger;
    }

    public function createConnection($dbParams)
    {
        // check params
        foreach (['host', 'database', 'user', '#password'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC
        ];

        $port = isset($dbParams['port']) ? $dbParams['port'] : '21050';
        $dsn = sprintf(
            "odbc:DSN=MyImpala;HOST=%s;PORT=%s;Database=%s;UID=%s;PWD=%s;AuthMech=%s",
            $dbParams['host'],
            $port,
            $dbParams['database'],
            $dbParams['user'],
            $dbParams['#password'],
            isset($dbParams['auth_mech'])?$dbParams['auth_mech']:3
        );

        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['#password'], $options);
        $pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);

        return $pdo;
    }

    public static function getAllowedTypes()
    {
        return self::$allowedTypes;
    }

    public function drop($tableName)
    {
        $this->execQuery(sprintf("DROP TABLE IF EXISTS %s", $this->escape($tableName)));
    }

    public function create(array $table)
    {
        $sql = "CREATE TABLE `{$table['dbName']}` (";

        $columns = $table['items'];
        foreach ($columns as $k => $col) {
            $type = strtolower($col['type']);
            if ($type == 'ignore') {
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


    public function write(CsvFile $csv, array $table)
    {
        // skip the header
        $csv->next();
        $csv->next();

        $columnsCount = count($csv->current());
        $rowsPerInsert = intval((1000 / $columnsCount) - 1);

        while ($csv->current() !== false) {
            $sql = sprintf('INSERT INTO %s VALUES ', $this->escape($table['dbName']));

            for ($i=0; $i<$rowsPerInsert && $csv->current() !== false; $i++) {
                $sql .= sprintf("(%s),", $this->getValuesClause($csv->current(), $table['items']));
                $csv->next();
            }
            // ditch the last coma
            $sql = substr($sql, 0, -1);

            var_dump($sql);
            $this->db->exec($sql);
        }
    }

    public function upsert(array $table, $targetTable)
    {
        $sourceTable = $table['dbName'];

        // create target table if not exists
        if (!$this->tableExists($targetTable)) {
            $destinationTable = $table;
            $destinationTable['dbName'] = $targetTable;
            $this->create($destinationTable);
        }

        $columns = array_map(function ($item) {
            if (strtolower($item['type']) != 'ignore') {
                return $this->escape($item['dbName']);
            }
        }, $table['items']);

        if (!empty($table['primaryKey'])) {
            // update data
            $joinClauseArr = [];
            foreach ($table['primaryKey'] as $index => $value) {
                $joinClauseArr[] = "a.{$value}=b.{$value}";
            }
            $joinClause = implode(' AND ', $joinClauseArr);

            $valuesClauseArr = [];
            foreach ($columns as $index => $column) {
                $valuesClauseArr[] = "a.{$column}=b.{$column}";
            }
            $valuesClause = implode(',', $valuesClauseArr);

            $query = "UPDATE a
                SET {$valuesClause}
                FROM {$targetTable} a
                INNER JOIN {$sourceTable} b ON {$joinClause}
            ";

            $this->execQuery($query);

            // delete updated from temp table
            $query = "DELETE a FROM {$sourceTable} a
                INNER JOIN {$targetTable} b ON {$joinClause}
            ";

            $this->execQuery($query);
        }

        // insert new data
        $columnsClause = implode(',', $columns);
        $query = "INSERT INTO {$targetTable} ({$columnsClause}) SELECT * FROM {$sourceTable}";
        $this->execQuery($query);

        // drop temp table
        $this->drop($sourceTable);
    }

    public function tableExists($tableName)
    {
        $tableArr = explode('.', $tableName);
        $tableName = isset($tableArr[1])?$tableArr[1]:$tableArr[0];

        try {
            $stmt = $this->db->query(sprintf("SELECT * FROM %s LIMIT 1", $this->escape($tableName)));
        } catch (\PDOException $e) {
            return false;
        }

        return true;
    }

    public function isTableValid(array $table, $ignoreExport = false)
    {
        // TODO: Implement isTableValid() method.

        return true;
    }

    private function execQuery($query)
    {
        $this->logger->info(sprintf("Executing query '%s'", $query));
        $this->db->exec($query);
    }

    public function showTables($dbName)
    {
        // TODO: Implement showTables() method.
    }

    public function getTableInfo($tableName)
    {
        // TODO: Implement getTableInfo() method.
    }

    private function escape($str)
    {
        return sprintf('`%s`', $str);
    }

    private function escapeSingleQuotes($str)
    {
        return preg_replace("/\'/", "\\'", $str);
    }

    private function getValuesClause($row, $columns)
    {
        $res = [];
        foreach ($row as $index => $value) {
            if (strtolower($columns[$index]['type']) == 'ignore') {
                continue;
            }
            if (is_numeric($value)) {
                $res[] = $value;
                continue;
            }
            $type = $columns[$index]['type'];
            if (null !== $columns[$index]['size']) {
                $type .= sprintf('(%s)', $columns[$index]['size']);
            }

            $res[] = sprintf("cast('%s' as %s)", $this->escapeSingleQuotes($value), $type);
        }

        return implode(',', $res);
    }
}

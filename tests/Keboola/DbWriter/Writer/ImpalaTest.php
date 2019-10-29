<?php

declare(strict_types=1);

namespace Keboola\Tests\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Test\BaseTest;
use Keboola\DbWriter\WriterInterface;

class ImpalaTest extends BaseTest
{
    /** @var string */
    private $rootDir = __DIR__ . '/../../../../';

    /** @var string */
    protected $dataDir = __DIR__ . '/../../../../tests/data';

    /** @var string  */
    private const DRIVER = 'impala';

    /** @var WriterInterface $writer */
    private $writer;

    /** @var array $config */
    private $config;

    public function setUp(): void
    {
        $this->config = $this->getConfig($this->rootDir . 'tests/data/' . self::DRIVER);
        $this->config['parameters']['writer_class'] = 'Impala';
        $this->writer = $this->getWriter($this->config['parameters']);
        $conn = $this->writer->getConnection();

        $tables = $this->config['parameters']['tables'];

        foreach ($tables as $table) {
            $conn->exec(sprintf('DROP TABLE IF EXISTS %s', $table['dbName']));
        }
    }

    public function testDrop(): void
    {
        $this->expectException('PDOException');
        $this->expectExceptionMessageRegExp('/Table does not exist/ui');

        $conn = $this->writer->getConnection();
        $conn->exec('CREATE TABLE IF NOT EXISTS dropMe (
          id INT,
          firstname VARCHAR(30),
          lastname VARCHAR(30)
        )');

        $this->writer->drop('dropMe');

        $stmt = $conn->query('SELECT * FROM dropMe');
        $stmt->fetch(\PDO::FETCH_ASSOC);
    }

    public function testCreate(): void
    {
        $tables = $this->config['parameters']['tables'];

        foreach ($tables as $table) {
            $this->writer->create($table);
        }

        /** @var \PDO $conn */
        $conn = $this->writer->getConnection();
        $stmt = $conn->query(sprintf('SELECT * FROM `%s`', $tables[0]['dbName']));
        $res = $stmt->fetchAll();

        $this->assertEmpty($res);
    }

    public function testWrite(): void
    {
        $tables = $this->config['parameters']['tables'];

        // simple table
        $table = $tables[0];
        $sourceTableId = $table['tableId'];
        $outputTableName = $table['dbName'];
        $sourceFilename = $this->dataDir . '/' . $sourceTableId . '.csv';

        $this->writer->drop($outputTableName);
        $this->writer->create($table);
        $this->writer->write(new CsvFile($sourceFilename), $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM $outputTableName");
        $res = (array) $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = (string) tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(['id','name','glasses']);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($sourceFilename, $resFilename);

        // @todo: Impala is terrible at escaping :(
        // table with special chars
//        $table = $tables[1];
//        $sourceTableId = $table['tableId'];
//        $outputTableName = $table['dbName'];
//        $sourceFilename = $this->dataDir . '/' . $sourceTableId . '.csv';
//
//        $this->writer->drop($outputTableName);
//        $this->writer->create($table);
//        $this->writer->write(new CsvFile($sourceFilename), $table);
//
//        $conn = $this->writer->getConnection();
//        $stmt = $conn->query('SELECT * FROM $outputTableName');
//        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);
//
//        $resFilename = tempnam('/tmp', 'db-wr-test-tmp-2');
//        $csv = new CsvFile($resFilename);
//        $csv->writeRow(['col1','col2']);
//        foreach ($res as $row) {
//            $csv->writeRow($row);
//        }
//
//        $this->assertFileEquals($sourceFilename, $resFilename);

        // ignored columns
        $table = $tables[0];
        $sourceTableId = $table['tableId'];
        $outputTableName = $table['dbName'];
        $sourceFilename = $this->dataDir . '/' . $sourceTableId . '.csv';

        $table['items'][2]['type'] = 'IGNORE';

        $this->writer->drop($outputTableName);
        $this->writer->create($table);
        $this->writer->write(new CsvFile($sourceFilename), $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM $outputTableName");
        $res = (array) $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resArr = [];
        foreach ($res as $row) {
            $resArr[] = array_values($row);
        }

        $srcArr = [];
        $csv = new CsvFile($sourceFilename);
        $csv->next();
        $csv->next();

        while ($csv->current()) {
            $currRow = $csv->current();
            unset($currRow[2]);
            $srcArr[] = array_values($currRow);
            $csv->next();
        }

        $this->assertEquals($srcArr, $resArr);
    }

    public function testGetAllowedTypes(): void
    {
        $allowedTypes = $this->writer->getAllowedTypes();

        $this->assertEquals([
            'bigint', 'boolean', 'char', 'decimal',
            'double', 'float', 'int', 'real',
            'smallint', 'string', 'timestamp',
            'tinyint', 'varchar',
        ], $allowedTypes);
    }

    /**
     * @todo
     */
//    public function testUpsert()
//    {
//        $conn = $this->writer->getConnection();
//        $tables = $this->config['parameters']['tables'];
//
//        $table = $tables[0];
//        $sourceFilename = $this->dataDir . '/' . $table['tableId'] . '.csv';
//        $targetTable = $table;
//        $table['dbName'] .= $table['incremental']?'_temp_' . uniqid():'';
//
//        // first write
//        $this->writer->create($targetTable);
//        $this->writer->write(new CsvFile($sourceFilename), $targetTable);
//
//        // second write
//        $sourceFilename = $this->dataDir . '/' . $table['tableId'] . '_increment.csv';
//        $this->writer->create($table);
//        $this->writer->write(new CsvFile($sourceFilename), $table);
//        $this->writer->upsert($table, $targetTable['dbName']);
//
//        $stmt = $conn->query('SELECT * FROM {$targetTable['dbName']}');
//        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);
//
//        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
//        $csv = new CsvFile($resFilename);
//        $csv->writeRow(['id', 'name', 'glasses']);
//        foreach ($res as $row) {
//            $csv->writeRow($row);
//        }
//
//        $expectedFilename = $this->dataDir . '/' . $table['tableId'] . '_merged.csv';
//
//        $this->assertFileEquals($expectedFilename, $resFilename);
//    }
}

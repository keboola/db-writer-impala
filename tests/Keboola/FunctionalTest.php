<?php

declare(strict_types=1);

namespace Keboola\Tests;

use Keboola\DbWriter\Test\BaseTest;
use Symfony\Component\Process\Process;

class FunctionalTest extends BaseTest
{
    /** @var string */
    private $rootDir = __DIR__ . '/../../';

    public function setUp(): void
    {
        // cleanup
        $config = json_decode(
            (string) file_get_contents($this->rootDir . 'tests/data/functional/config.json'),
            true
        );
        $config['parameters']['writer_class'] = 'Impala';
        $writer = $this->getWriter($config['parameters']);

        foreach ($config['parameters']['tables'] as $table) {
            $writer->drop($table['dbName']);
        }
    }

    public function testRun(): void
    {
        $command = 'php ' . $this->rootDir . 'run.php --data=' . $this->rootDir . 'tests/data/functional 2>&1';
        var_dump($command);
        $process = new Process($command);
        $process->run();
        $this->assertEquals(0, $process->getExitCode());
    }

    public function testTestConnection(): void
    {
        $configPath = $this->rootDir . 'tests/data/functional/config.json';
        $config = json_decode((string) file_get_contents($configPath), true);
        $config['action'] = 'testConnection';
        unlink($configPath);
        file_put_contents($configPath, json_encode($config));

        $commnand = 'php ' . $this->rootDir . 'run.php --data=' . $this->rootDir . 'tests/data/functional 2>&1';
        $process = new Process($commnand);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());

        $data = json_decode($process->getOutput(), true);

        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }
}

<?php

declare(strict_types=1);

namespace Keboola\Tests;

use Keboola\DbWriter\Test\BaseTest;
use Symfony\Component\Process\Process;

class BaseFunctionalTest extends BaseTest
{
    /** @var string */
    protected $rootDir = __DIR__ . '/../../';

    public function testRun(): void
    {
        $command = 'php ' . $this->rootDir . 'run.php --data=' . $this->rootDir . 'tests/data/functional 2>&1';
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

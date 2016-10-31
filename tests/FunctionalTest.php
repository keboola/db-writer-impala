<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 27/10/16
 * Time: 17:20
 */

namespace Keboola\DbWriter\Writer\Tests;

use Keboola\DbWriter\Test\BaseTest;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class FunctionalTest extends BaseTest
{
    public function setUp()
    {
        // cleanup
        $config = Yaml::parse(file_get_contents(ROOT_PATH . 'tests/data/functional/config.yml'));
        $config['parameters']['writer_class'] = 'Impala';
        $writer = $this->getWriter($config['parameters']);

        foreach ($config['parameters']['tables'] as $table) {
            $writer->drop($table['dbName']);
        }
    }

    public function testRun()
    {
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . ROOT_PATH . 'tests/data/functional 2>&1');
        $process->run();
        $this->assertEquals(0, $process->getExitCode());
    }

    public function testTestConnection()
    {
        $yaml = new Yaml();
        $configPath = ROOT_PATH . 'tests/data/functional/config.yml';
        $config = $yaml->parse(file_get_contents($configPath));
        $config['action'] = 'testConnection';
        unlink($configPath);
        file_put_contents($configPath, $yaml->dump($config));

        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . ROOT_PATH . 'tests/data/functional 2>&1');
        $process->run();

        $this->assertEquals(0, $process->getExitCode());

        $data = json_decode($process->getOutput(), true);

        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }
}

<?php

declare(strict_types=1);

namespace Keboola\Tests;

class FunctionalTest extends BaseFunctionalTest
{

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
}

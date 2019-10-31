<?php

declare(strict_types=1);

namespace Keboola\Tests;

class FunctionalRowTest extends BaseFunctionalTest
{

    public function setUp(): void
    {
        // cleanup
        $config = json_decode(
            (string) file_get_contents($this->rootDir . 'tests/data/functionalRow/config.json'),
            true
        );
        $config['parameters']['writer_class'] = 'Impala';
        $writer = $this->getWriter($config['parameters']);
        $writer->drop($config['parameters']['tableId']);
    }
}

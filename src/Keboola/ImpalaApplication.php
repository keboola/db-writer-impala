<?php

namespace Keboola\DbWriter;

use Keboola\DbWriter\Impala\Configuration\ImpalaActionConfigRowDefinition;
use Keboola\DbWriter\Impala\Configuration\ImpalaConfigDefinition;
use Keboola\DbWriter\Impala\Configuration\ImpalaConfigRowDefinition;

class ImpalaApplication extends Application
{
    public function __construct(array $config, Logger $logger)
    {
        $action = !is_null($config['action']) ?: 'run';
        if (isset($config['parameters']['tables'])) {
            $configDefinition = new ImpalaConfigDefinition();
        } else {
            if ($action === 'run') {
                $configDefinition = new ImpalaConfigRowDefinition();
            } else {
                $configDefinition = new ImpalaActionConfigRowDefinition();
            }
        }
        parent::__construct($config, $logger, $configDefinition);
    }
}
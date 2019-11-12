<?php

declare(strict_types=1);

use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\ImpalaApplication;

define('APP_NAME', 'wr-db-impala');
define('ROOT_PATH', __DIR__);

require_once(dirname(__FILE__) . '/vendor/autoload.php');

$logger = new Logger(APP_NAME);

try {
    $arguments = getopt('d::', ['data::']);
    if (!isset($arguments['data'])) {
        throw new UserException('Data folder not set.');
    }
    $config = json_decode((string) file_get_contents($arguments['data'] . '/config.json'), true);
    $config['parameters']['data_dir'] = $arguments['data'];
    $config['parameters']['writer_class'] = 'Impala';

    $app = new ImpalaApplication($config, $logger);

    echo $app->run();
} catch (UserException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());
    exit(1);
} catch (ApplicationException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());
    exit($e->getCode() > 1 ? $e->getCode(): 2);
} catch (\Throwable $e) {
    $logger->log('error', $e->getMessage(), [
        'errFile' => $e->getFile(),
        'errLine' => $e->getLine(),
        'trace' => $e->getTrace(),
    ]);
    exit(2);
}
exit(0);

<?php

declare(strict_types=1);

use Keboola\Component\Logger;
use Keboola\Component\UserException;
use Keboola\TelemetryData\Component;

require __DIR__ . '/../vendor/autoload.php';

$logger = new Logger();
try {
    $app = new Component($logger);
    $app->execute();
    exit(0);
} catch (UserException $e) {
    $logger->error($e->getMessage());
    exit(1);
} catch (Throwable $e) {
    $logger->critical(
        $e::class . ':' . $e->getMessage(),
        [
            'errFile' => $e->getFile(),
            'errLine' => $e->getLine(),
            'errCode' => $e->getCode(),
            'errTrace' => $e->getTraceAsString(),
            'errPrevious' => is_object($e->getPrevious()) ? $e->getPrevious()::class : '',
        ],
    );
    exit(2);
}

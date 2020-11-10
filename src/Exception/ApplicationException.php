<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\Exception;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use \Exception;

class ApplicationException extends Exception implements ApplicationExceptionInterface
{

}

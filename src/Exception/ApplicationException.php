<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\Exception;

use Exception;
use Keboola\CommonExceptions\ApplicationExceptionInterface;

class ApplicationException extends Exception implements ApplicationExceptionInterface
{

}

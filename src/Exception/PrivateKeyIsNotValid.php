<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\Exception;

use Exception;
use Keboola\CommonExceptions\ApplicationExceptionInterface;

class PrivateKeyIsNotValid extends Exception implements ApplicationExceptionInterface
{

}

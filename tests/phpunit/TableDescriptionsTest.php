<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\Tests;

use Keboola\TelemetryData\TableDescriptions;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class TableDescriptionsTest extends TestCase
{
    public function testActiveTableHasDescription(): void
    {
        Assert::assertSame(
            'This table shows main and development [branches](https://help.keboola.com/components/branches/) '
            . 'in the project.',
            TableDescriptions::get('kbc_branch'),
        );
        Assert::assertSame(TableDescriptions::get('kbc_branch'), TableDescriptions::get('KBC_BRANCH'));
    }

    public function testDeprecatedOrUnknownTableHasNoDescription(): void
    {
        Assert::assertNull(TableDescriptions::get('kbc_workspace'));
        Assert::assertNull(TableDescriptions::get('unknown_table'));
    }
}

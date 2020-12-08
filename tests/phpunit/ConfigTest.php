<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\Tests;

use Keboola\TelemetryData\Config;
use Keboola\TelemetryData\ConfigDefinition;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Iterator;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigTest extends TestCase
{
    /** @dataProvider validConfigDataProvider */
    public function testValidConfig(array $configData, array $expectedData): void
    {
        $config = new Config(['parameters' => $configData], new ConfigDefinition());

        Assert::assertEquals(
            ['parameters' => $expectedData],
            $config->getData()
        );
    }

    public function testInvalidMode(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'The value "unknownMode" is not allowed for path "root.parameters.mode". ' .
            'Permissible values: "project", "organization"'
        );
        new Config(
            [
                'parameters' => [
                    'mode' => 'unknownMode',
                ],
            ],
            new ConfigDefinition()
        );
    }

    public function validConfigDataProvider(): Iterator
    {
        yield [
            [],
            ['mode' => Config::MODE_PROJECT, 'incrementalFetching' => false],
        ];

        yield [
            ['incrementalFetching' => false],
            ['mode' => Config::MODE_PROJECT, 'incrementalFetching' => false],
        ];

        yield [
            ['incrementalFetching' => true],
            ['mode' => Config::MODE_PROJECT, 'incrementalFetching' => true],
        ];

        yield [
            ['mode' => Config::MODE_PROJECT],
            ['mode' => Config::MODE_PROJECT, 'incrementalFetching' => false],
        ];

        yield [
            ['mode' => Config::MODE_ORGANIZATION],
            ['mode' => Config::MODE_ORGANIZATION, 'incrementalFetching' => false],
        ];

        yield [
            ['mode' => Config::MODE_PROJECT, 'incrementalFetching' => true],
            ['mode' => Config::MODE_PROJECT, 'incrementalFetching' => true],
        ];

        yield [
            ['mode' => Config::MODE_ORGANIZATION, 'incrementalFetching' => true],
            ['mode' => Config::MODE_ORGANIZATION, 'incrementalFetching' => true],
        ];
    }
}

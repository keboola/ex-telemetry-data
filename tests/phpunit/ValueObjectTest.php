<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\Tests;

use Iterator;
use Keboola\TelemetryData\ValueObject\Column;
use Keboola\TelemetryData\ValueObject\Table;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class ValueObjectTest extends TestCase
{
    /**
     * @dataProvider invalidColumnsConfig
     * @param string[] $columns
     * @param string[] $missingColumns
     */
    public function testTableObject(array $columns, array $missingColumns): void
    {
        $table = Table::buildFromArray(
            [
                'schema_name' => 'test_schema',
                'name' => 'test_name',
            ],
        );

        Assert::assertEquals('test_schema', $table->getSchema());
        Assert::assertEquals('test_name', $table->getName());

        foreach ($columns as $columnName) {
            $column = Column::buildFromArray(
                [
                    'COLUMN_NAME' => $columnName,
                    'CHARACTER_MAXIMUM_LENGTH' => 10,
                    'NUMERIC_PRECISION' => 20,
                    'NUMERIC_SCALE' => 30,
                    'IS_NULLABLE' => 'YES',
                    'DATA_TYPE' => 'varchar',
                    'TABLE_SCHEMA' => 'test_schema',
                    'TABLE_NAME' => 'test_name',
                ],
            );
            $table->addColumn($column);
        }

        Assert::assertEquals($missingColumns, $table->getMissingRequiredColumns());
    }

    public function testColumnObject(): void
    {
        $column = Column::buildFromArray(
            [
                'COLUMN_NAME' => Column::PROJECT_SINGLE_NAME,
                'CHARACTER_MAXIMUM_LENGTH' => 10,
                'NUMERIC_PRECISION' => 20,
                'NUMERIC_SCALE' => 30,
                'IS_NULLABLE' => 'YES',
                'DATA_TYPE' => 'varchar',
                'TABLE_SCHEMA' => 'test_schema',
                'TABLE_NAME' => 'test_name',
            ],
        );

        Assert::assertEquals(Column::PROJECT_SINGLE_NAME, $column->getName());
        Assert::assertEquals(10, $column->getCharacterMaximumLength());
        Assert::assertEquals(20, $column->getNumericPrecision());
        Assert::assertEquals(30, $column->getNumericScale());
        Assert::assertTrue($column->isNullable());
        Assert::assertEquals('varchar', $column->getDataType());
        Assert::assertEquals('test_schema', $column->getTableSchema());
        Assert::assertEquals('test_name', $column->getTableName());
    }

    public function invalidColumnsConfig(): Iterator
    {
        yield [
            [],
            [
                Column::PROJECT_SINGLE_NAME,
                Column::STACK_SINGLE_NAME,
                Column::PROJECT_COMPANY_NAME,
                Column::STACK_COMPANY_NAME,
                Column::PROJECT_ACTIVITY_CENTER_NAME,
                Column::STACK_ACTIVITY_CENTER_NAME,
                Column::INCREMENTAL_NAME,
            ],
        ];
        yield [
            [
                Column::STACK_COMPANY_NAME,
                Column::STACK_SINGLE_NAME,
                Column::STACK_ACTIVITY_CENTER_NAME,
                Column::PROJECT_COMPANY_NAME,
                Column::PROJECT_SINGLE_NAME,
                Column::PROJECT_ACTIVITY_CENTER_NAME,
            ],
            [Column::INCREMENTAL_NAME],
        ];
        yield [
            [
                Column::STACK_COMPANY_NAME,
                Column::STACK_SINGLE_NAME,
                Column::STACK_ACTIVITY_CENTER_NAME,
                Column::PROJECT_COMPANY_NAME,
                Column::PROJECT_ACTIVITY_CENTER_NAME,
                Column::INCREMENTAL_NAME,
            ],
            [Column::PROJECT_SINGLE_NAME],
        ];
        yield [
            [
                Column::STACK_COMPANY_NAME,
                Column::STACK_SINGLE_NAME,
                Column::STACK_ACTIVITY_CENTER_NAME,
                Column::PROJECT_SINGLE_NAME,
                Column::PROJECT_ACTIVITY_CENTER_NAME,
                Column::INCREMENTAL_NAME,
            ],
            [Column::PROJECT_COMPANY_NAME],
        ];
        yield [
            [
                Column::STACK_COMPANY_NAME,
                Column::STACK_ACTIVITY_CENTER_NAME,
                Column::PROJECT_COMPANY_NAME,
                Column::PROJECT_SINGLE_NAME,
                Column::PROJECT_ACTIVITY_CENTER_NAME,
                Column::INCREMENTAL_NAME,
            ],
            [Column::STACK_SINGLE_NAME],
        ];
        yield [
            [
                Column::STACK_SINGLE_NAME,
                Column::STACK_ACTIVITY_CENTER_NAME,
                Column::PROJECT_COMPANY_NAME,
                Column::PROJECT_SINGLE_NAME,
                Column::PROJECT_ACTIVITY_CENTER_NAME,
                Column::INCREMENTAL_NAME,
            ],
            [Column::STACK_COMPANY_NAME],
        ];
        yield [
            [
                Column::STACK_COMPANY_NAME,
                Column::STACK_SINGLE_NAME,
                Column::PROJECT_COMPANY_NAME,
                Column::PROJECT_SINGLE_NAME,
                Column::PROJECT_ACTIVITY_CENTER_NAME,
                Column::INCREMENTAL_NAME,
            ],
            [Column::STACK_ACTIVITY_CENTER_NAME],
        ];
        yield [
            [
                Column::STACK_COMPANY_NAME,
                Column::STACK_SINGLE_NAME,
                Column::STACK_ACTIVITY_CENTER_NAME,
                Column::PROJECT_COMPANY_NAME,
                Column::PROJECT_SINGLE_NAME,
                Column::INCREMENTAL_NAME,
            ],
            [Column::PROJECT_ACTIVITY_CENTER_NAME],
        ];
    }
}

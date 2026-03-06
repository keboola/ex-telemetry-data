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

    /**
     * @dataProvider typesWithoutLengthProvider
     */
    public function testGetLengthReturnsNullForTypesWithoutLength(string $dataType): void
    {
        $column = Column::buildFromArray(
            [
                'COLUMN_NAME' => 'test_column',
                'CHARACTER_MAXIMUM_LENGTH' => 0,
                'NUMERIC_PRECISION' => 0,
                'NUMERIC_SCALE' => 0,
                'IS_NULLABLE' => 'YES',
                'DATA_TYPE' => $dataType,
                'TABLE_SCHEMA' => 'test_schema',
                'TABLE_NAME' => 'test_name',
            ],
        );

        Assert::assertNull($column->getLength());
    }

    public function typesWithoutLengthProvider(): Iterator
    {
        yield 'TEXT' => ['TEXT'];
        yield 'MEDIUMTEXT' => ['MEDIUMTEXT'];
        yield 'LONGTEXT' => ['LONGTEXT'];
        yield 'BOOLEAN' => ['BOOLEAN'];
        yield 'DATE' => ['DATE'];
        yield 'DATETIME' => ['DATETIME'];
        yield 'TIME' => ['TIME'];
        yield 'TIMESTAMP' => ['TIMESTAMP'];
        yield 'TIMESTAMP_LTZ' => ['TIMESTAMP_LTZ'];
        yield 'TIMESTAMP_NTZ' => ['TIMESTAMP_NTZ'];
        yield 'TIMESTAMP_TZ' => ['TIMESTAMP_TZ'];
        yield 'VARIANT' => ['VARIANT'];
        yield 'OBJECT' => ['OBJECT'];
        yield 'ARRAY' => ['ARRAY'];
        yield 'GEOGRAPHY' => ['GEOGRAPHY'];
        yield 'GEOMETRY' => ['GEOMETRY'];
    }

    /**
     * @dataProvider typesWithLengthProvider
     */
    public function testGetLengthReturnsArrayForTypesWithLength(string $dataType): void
    {
        $column = Column::buildFromArray(
            [
                'COLUMN_NAME' => 'test_column',
                'CHARACTER_MAXIMUM_LENGTH' => 10,
                'NUMERIC_PRECISION' => 20,
                'NUMERIC_SCALE' => 30,
                'IS_NULLABLE' => 'YES',
                'DATA_TYPE' => $dataType,
                'TABLE_SCHEMA' => 'test_schema',
                'TABLE_NAME' => 'test_name',
            ],
        );

        $length = $column->getLength();
        Assert::assertIsArray($length);
        Assert::assertSame(10, $length['character_maximum']);
        Assert::assertSame(20, $length['numeric_precision']);
        Assert::assertSame(30, $length['numeric_scale']);
    }

    public function typesWithLengthProvider(): Iterator
    {
        yield 'VARCHAR' => ['VARCHAR'];
        yield 'NUMBER' => ['NUMBER'];
        yield 'BINARY' => ['BINARY'];
        yield 'FLOAT' => ['FLOAT'];
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

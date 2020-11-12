<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\Tests;

use Keboola\TelemetryData\ValueObject\Column;
use Keboola\TelemetryData\ValueObject\Table;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class ValueObjectTest extends TestCase
{
    public function testTableObject(): void
    {
        $table = Table::buildFromArray(
            [
                'TABLE_SCHEMA' => 'test_schema',
                'TABLE_NAME' => 'test_name',
            ]
        );

        Assert::assertEquals('test_schema', $table->getSchema());
        Assert::assertEquals('test_name', $table->getName());
        Assert::assertFalse($table->hasProjectStackColumn());
        Assert::assertFalse($table->hasProjectIdColumn());

        $column = Column::buildFromArray(
            [
                'COLUMN_NAME' => Column::PROJECT_STACK_NAME,
                'CHARACTER_MAXIMUM_LENGTH' => 10,
                'NUMERIC_PRECISION' => 20,
                'NUMERIC_SCALE' => 30,
                'IS_NULLABLE' => 'YES',
                'DATA_TYPE' => 'varchar',
                'TABLE_SCHEMA' => 'test_schema',
                'TABLE_NAME' => 'test_name',
            ]
        );
        $table->addColumn($column);

        Assert::assertTrue($table->hasProjectStackColumn());
        Assert::assertFalse($table->hasProjectIdColumn());

        $column = Column::buildFromArray(
            [
                'COLUMN_NAME' => Column::PROJECT_ID_NAME,
                'CHARACTER_MAXIMUM_LENGTH' => 10,
                'NUMERIC_PRECISION' => 20,
                'NUMERIC_SCALE' => 30,
                'IS_NULLABLE' => 'YES',
                'DATA_TYPE' => 'varchar',
                'TABLE_SCHEMA' => 'test_schema',
                'TABLE_NAME' => 'test_name',
            ]
        );
        $table->addColumn($column);

        Assert::assertTrue($table->hasProjectStackColumn());
        Assert::assertTrue($table->hasProjectIdColumn());

        Assert::assertCount(2, $table->getColumns());
    }

    public function testColumnObject(): void
    {
        $column = Column::buildFromArray(
            [
                'COLUMN_NAME' => Column::PROJECT_ID_NAME,
                'CHARACTER_MAXIMUM_LENGTH' => 10,
                'NUMERIC_PRECISION' => 20,
                'NUMERIC_SCALE' => 30,
                'IS_NULLABLE' => 'YES',
                'DATA_TYPE' => 'varchar',
                'TABLE_SCHEMA' => 'test_schema',
                'TABLE_NAME' => 'test_name',
            ]
        );

        Assert::assertEquals(Column::PROJECT_ID_NAME, $column->getName());
        Assert::assertEquals(10, $column->getCharacterMaximumLength());
        Assert::assertEquals(20, $column->getNumericPrecision());
        Assert::assertEquals(30, $column->getNumericScale());
        Assert::assertTrue($column->isNullable());
        Assert::assertEquals('varchar', $column->getDataType());
        Assert::assertEquals('test_schema', $column->getTableSchema());
        Assert::assertEquals('test_name', $column->getTableName());
    }
}

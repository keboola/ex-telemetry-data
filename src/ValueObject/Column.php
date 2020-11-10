<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\ValueObject;

class Column
{
    public const PROJECT_STACK_NAME = 'PROJECT_STACK';

    public const PROJECT_ID_NAME = 'PROJECT_ID';

    private string $name;

    private int $characterMaximumLength;

    private int $numericPrecision;

    private int $numericScale;

    private bool $isNullable;

    private string $dataType;

    private string $tableSchema;

    private string $tableName;

    public static function buildFromArray(array $data): self
    {
        return new self(
            $data['COLUMN_NAME'],
            (int) $data['CHARACTER_MAXIMUM_LENGTH'],
            (int) $data['NUMERIC_PRECISION'],
            (int) $data['NUMERIC_SCALE'],
            $data['IS_NULLABLE'] === 'NO' ? false : true,
            $data['DATA_TYPE'],
            $data['TABLE_SCHEMA'],
            $data['TABLE_NAME']
        );
    }

    public function __construct(
        string $name,
        int $characterMaximumLength,
        int $numericPrecision,
        int $numericScale,
        bool $isNullable,
        string $dataType,
        string $tableSchema,
        string $tableName
    ) {
        $this->name = $name;
        $this->characterMaximumLength = $characterMaximumLength;
        $this->numericPrecision = $numericPrecision;
        $this->numericScale = $numericScale;
        $this->isNullable = $isNullable;
        $this->dataType = $dataType;
        $this->tableSchema = $tableSchema;
        $this->tableName = $tableName;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getTableSchema(): string
    {
        return $this->tableSchema;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    public function getCharacterMaximumLength(): int
    {
        return $this->characterMaximumLength;
    }

    public function getNumericPrecision(): int
    {
        return $this->numericPrecision;
    }

    public function getNumericScale(): int
    {
        return $this->numericScale;
    }

    public function isNullable(): bool
    {
        return $this->isNullable;
    }

    public function getDataType(): string
    {
        return $this->dataType;
    }

    public function getLength(): array
    {
        return [
            'character_maximum' => $this->getCharacterMaximumLength(),
            'numeric_precision' => $this->getNumericPrecision(),
            'numeric_scale' => $this->getNumericScale(),
        ];
    }
}

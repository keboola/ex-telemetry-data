<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\ValueObject;

class Column
{
    public const PROJECT_SINGLE_NAME = 'dst_proj_single';

    public const STACK_SINGLE_NAME = 'dst_stack_single';

    public const PROJECT_COMPANY_NAME = 'dst_proj_company';

    public const STACK_COMPANY_NAME = 'dst_stack_company';

    public const INCREMENTAL_NAME = 'dst_timestamp';

    private const TYPE_WITHOUT_LENGTH = [
        'TEXT',
        'MEDIUMTEXT',
        'LONGTEXT',
    ];

    private string $name;

    private int $characterMaximumLength;

    private int $numericPrecision;

    private int $numericScale;

    private bool $isNullable;

    private bool $isPrimaryKey = false;

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

    public function setIsPrimaryKey(bool $isPrimaryKey): self
    {
        $this->isPrimaryKey = $isPrimaryKey;
        return $this;
    }

    public function isPrimaryKey(): bool
    {
        return $this->isPrimaryKey;
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

    public function getLength(): ?array
    {
        if (in_array(strtoupper($this->getDataType()), self::TYPE_WITHOUT_LENGTH)) {
            return null;
        }
        return [
            'character_maximum' => $this->getCharacterMaximumLength(),
            'numeric_precision' => $this->getNumericPrecision(),
            'numeric_scale' => $this->getNumericScale(),
        ];
    }
}

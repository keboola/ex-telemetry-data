<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\ValueObject;

class Column
{
    public const PROJECT_SINGLE_NAME = 'dst_proj_single';

    public const STACK_SINGLE_NAME = 'dst_stack_single';

    public const PROJECT_COMPANY_NAME = 'dst_proj_company';

    public const STACK_COMPANY_NAME = 'dst_stack_company';

    public const PROJECT_ACTIVITY_CENTER_NAME = 'dst_proj_ac';

    public const STACK_ACTIVITY_CENTER_NAME = 'dst_stack_ac';

    public const INCREMENTAL_NAME = 'dst_timestamp';

    private const TYPE_WITHOUT_LENGTH = [
        'TEXT',
        'MEDIUMTEXT',
        'LONGTEXT',
    ];

    private bool $isPrimaryKey = false;

    /**
     * @param array{
     *     COLUMN_NAME: string,
     *     CHARACTER_MAXIMUM_LENGTH: string|int,
     *     NUMERIC_PRECISION: string|int,
     *     NUMERIC_SCALE: string|int,
     *     IS_NULLABLE: string,
     *     DATA_TYPE: string,
     *     TABLE_SCHEMA: string,
     *     TABLE_NAME: string,
     * } $data
     */
    public static function buildFromArray(array $data): self
    {
        return new self(
            $data['COLUMN_NAME'],
            (int) $data['CHARACTER_MAXIMUM_LENGTH'],
            (int) $data['NUMERIC_PRECISION'],
            (int) $data['NUMERIC_SCALE'],
            $data['IS_NULLABLE'] !== 'NO',
            $data['DATA_TYPE'],
            $data['TABLE_SCHEMA'],
            $data['TABLE_NAME'],
        );
    }

    public function __construct(
        private string $name,
        private int $characterMaximumLength,
        private int $numericPrecision,
        private int $numericScale,
        private bool $isNullable,
        private string $dataType,
        private string $tableSchema,
        private string $tableName,
    ) {
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

    /**
     * @return array{
     *     character_maximum: int|null,
     *     numeric_precision: int|null,
     *     numeric_scale: int|null
     * }
     */
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

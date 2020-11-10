<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\ValueObject;

class Table
{
    private string $schema;

    private string $name;

    private bool $hasProjectIdColumn = false;

    private bool $hasProjectStackColumn = false;

    /** @var Column[] $columns */
    private array $columns = [];

    public static function buildFromArray(array $data): self
    {
        return new self(
            $data['TABLE_SCHEMA'],
            $data['TABLE_NAME']
        );
    }

    public function __construct(string $schema, string $name)
    {
        $this->schema = $schema;
        $this->name = $name;
    }

    public function addColumn(Column $column): void
    {
        if ($column->getName() === Column::PROJECT_ID_NAME) {
            $this->hasProjectIdColumn = true;
        }
        if ($column->getName() === Column::PROJECT_STACK_NAME) {
            $this->hasProjectStackColumn = true;
        }
        $this->columns[] = $column;
    }

    public function getSchema(): string
    {
        return $this->schema;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function hasProjectIdColumn(): bool
    {
        return $this->hasProjectIdColumn;
    }

    public function hasProjectStackColumn(): bool
    {
        return $this->hasProjectStackColumn;
    }

    public function getColumns(): array
    {
        return $this->columns;
    }
}

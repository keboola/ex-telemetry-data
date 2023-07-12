<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\ValueObject;

class Table
{
    private string $schema;

    private string $name;

    private array $requiredTableColumns = [
        'dst_proj_single',
        'dst_stack_single',
        'dst_proj_company',
        'dst_stack_company',
        'dst_proj_ac',
        'dst_stack_ac',
        'dst_timestamp',
    ];

    /** @var Column[] $columns */
    private array $columns = [];

    public static function buildFromArray(array $data): self
    {
        return new self(
            $data['schema_name'],
            $data['name']
        );
    }

    public function __construct(string $schema, string $name)
    {
        $this->schema = $schema;
        $this->name = $name;
    }

    public function addColumn(Column $column): void
    {
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

    public function getColumns(): array
    {
        return $this->columns;
    }

    public function getMissingRequiredColumns(): array
    {
        return array_values(array_diff(
            $this->requiredTableColumns,
            array_map(fn(Column $v) => $v->getName(), $this->columns)
        ));
    }
}

<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\ValueObject;

class Table
{
    /**
     * @var string[]
     */
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

    /**
     * @param array{schema_name:string,name:string} $data
     */
    public static function buildFromArray(array $data): self
    {
        return new self(
            $data['schema_name'],
            $data['name'],
        );
    }

    public function __construct(private string $schema, private string $name)
    {
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

    /**
     * @return Column[]
     */
    public function getColumns(): array
    {
        return $this->columns;
    }

    /**
     * @return string[]
     */
    public function getMissingRequiredColumns(): array
    {
        return array_values(array_diff(
            $this->requiredTableColumns,
            array_map(fn(Column $v): string => $v->getName(), $this->columns),
        ));
    }
}

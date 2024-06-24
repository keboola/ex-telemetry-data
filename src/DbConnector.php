<?php

declare(strict_types=1);

namespace Keboola\TelemetryData;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\DriverException;
use Keboola\Component\UserException;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TelemetryData\ValueObject\Column;
use Keboola\TelemetryData\ValueObject\Table;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use SplFileInfo;

class DbConnector
{
    private Connection $connection;

    private SplFileInfo $snowSqlConfigFile;

    public function __construct(private Config $config, private LoggerInterface $logger)
    {
        $this->connection = $this->createConnection();
        $this->snowSqlConfigFile = $this->createSnowSqlConfig();
    }

    private function createConnection(): Connection
    {
        try {
            $connection = SnowflakeConnectionFactory::getConnection(
                $this->config->getDbHost(),
                $this->config->getDbUser(),
                $this->config->getDbPassword(),
                [
                    'port' => $this->config->getDbPort(),
                    'warehouse' => $this->config->getDbWarehouse(),
                    'database' => $this->config->getDbDatabase(),
                ],
            );
            $connection->executeStatement(
                sprintf(
                    'USE SCHEMA %s',
                    SnowflakeQuote::quoteSingleIdentifier($this->config->getDbSchema()),
                ),
            );
        } catch (DriverException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }

        return $connection;
    }

    public function getSnowSqlConfigFile(): SplFileInfo
    {
        return $this->snowSqlConfigFile;
    }

    /**
     * @param Table[]|null $whiteList
     * @return Table[]
     */
    public function getTables(?array $whiteList = null): array
    {
        if (is_array($whiteList) && !$whiteList) {
            return [];
        }

        $tables = $this->queryTables($whiteList);
        if ($tables === []) {
            return [];
        }

        $tableObjects = [];
        $sqlWhereElements = [];
        foreach ($tables as $table) {
            $tableObject = Table::buildFromArray(
                $table,
            );

            $tableId = sprintf(
                '%s.%s',
                $tableObject->getSchema(),
                $tableObject->getName(),
            );

            $tableObjects[$tableId] = $tableObject;
            $sqlWhereElements[] = sprintf(
                '(table_schema = %s AND table_name = %s)',
                SnowflakeQuote::quote($table['schema_name']),
                SnowflakeQuote::quote($table['name']),
            );
        }

        $primaryKeys = $this->queryPrimaryKeys();

        foreach ($this->queryColumns($sqlWhereElements) as $column) {
            $columnObject = Column::buildFromArray($column);
            $tableId = sprintf(
                '%s.%s',
                $columnObject->getTableSchema(),
                $columnObject->getTableName(),
            );

            if (isset($primaryKeys[$tableId])) {
                $columnObject->setIsPrimaryKey(in_array($columnObject->getName(), $primaryKeys[$tableId]));
            }

            $tableObjects[$tableId]->addColumn($columnObject);
        }
        foreach ($tableObjects as $tableId => $table) {
            $missingColumns = $table->getMissingRequiredColumns();
            if ($missingColumns !== []) {
                unset($tableObjects[$tableId]);
                $this->logger->info(sprintf(
                    'Missing "%s" columns for table "%s".',
                    implode(', ', $missingColumns),
                    $table->getName(),
                ));
            }
        }

        return $tableObjects;
    }

    public function cleanupTableStage(string $tmpTableName): void
    {
        $sql = sprintf('REMOVE @~/%s;', $tmpTableName);
        $this->connection->executeStatement($sql);
    }

    /**
     * @return array<int, array<string, string>>
     */
    public function fetchAll(string $sql): array
    {
        //@phpstan-ignore-next-line
        return $this->connection->fetchAllAssociative($sql);
    }

    public function fetchOneStringOrNull(string $sql): string|null
    {
        $result = $this->connection->fetchOne($sql);
        if ($result === false) {
            return null;
        }
        assert(is_string($result));
        return $result;
    }

    private function createSnowSqlConfig(): SplFileInfo
    {
        $hostParts = explode('.', $this->config->getDbHost());
        $accountName = implode('.', array_slice($hostParts, 0, count($hostParts) - 2));

        $cliConfig[] = '';
        $cliConfig[] = '[options]';
        $cliConfig[] = 'exit_on_error = true';
        $cliConfig[] = '';
        $cliConfig[] = '[connections.downloader]';
        $cliConfig[] = sprintf('accountname = "%s"', $accountName);
        $cliConfig[] = sprintf('username = "%s"', $this->config->getDbUser());
        $cliConfig[] = sprintf('password = "%s"', $this->config->getDbPassword());
        $cliConfig[] = sprintf('dbname = "%s"', $this->config->getDbDatabase());
        $cliConfig[] = sprintf('warehousename = "%s"', $this->config->getDbWarehouse());
        $cliConfig[] = sprintf('schemaname = "%s"', $this->config->getDbSchema());

        $file = (new Temp())->createFile('snowsql.config');
        file_put_contents($file->getPathname(), implode("\n", $cliConfig));

        return $file;
    }

    /**
     * @param Table[]|null $whiteList
     * @return array<array{schema_name:string,name:string}>
     */
    private function queryTables(?array $whiteList): array
    {
        /** @var array<array{schema_name:string,name:string}> $tables */
        $tables = $this->connection->fetchAllAssociative('SHOW TABLES IN SCHEMA');

        $filteredTables = array_filter($tables, fn($v): bool => !$this->shouldTableBeSkipped($v, $whiteList));

        usort($filteredTables, fn($item1, $item2): int => strnatcmp($item1['name'], $item2['name']));

        return $filteredTables;
    }

    /**
     * @param array{schema_name:string,name:string} $table
     * @param Table[]|null $whiteList
     */
    private function shouldTableBeSkipped(array $table, ?array $whiteList): bool
    {
        $isFromInformationSchema = $table['schema_name'] === 'INFORMATION_SCHEMA';
        $isStageTable = str_starts_with($table['name'], 'staging');
        $isNotFromWhiteList = false;
        if ($whiteList) {
            $filteredWhiteList = array_filter(
                $whiteList,
                fn(Table $v): bool => $v->getSchema() === $table['schema_name'] && $v->getName() === $table['name'],
            );
            $isNotFromWhiteList = $filteredWhiteList === [];
        }
        return $isFromInformationSchema || $isNotFromWhiteList || $isStageTable;
    }

    /**
     * @param string[] $queryTables
     * @return array<int, array{
     *      COLUMN_NAME: string,
     *      CHARACTER_MAXIMUM_LENGTH: string|int,
     *      NUMERIC_PRECISION: string|int,
     *      NUMERIC_SCALE: string|int,
     *      IS_NULLABLE: string,
     *      DATA_TYPE: string,
     *      TABLE_SCHEMA: string,
     *      TABLE_NAME: string,
     *  }>
     */
    private function queryColumns(array $queryTables): array
    {
        $sqlWhereClause = sprintf('WHERE %s', implode(' OR ', $queryTables));

        $sql = sprintf(
            'SELECT * FROM information_schema.columns %s ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION',
            $sqlWhereClause,
        );

        //@phpstan-ignore-next-line
        return $this->connection->fetchAllAssociative($sql);
    }

    /**
     * @return array<string, string[]>
     */
    private function queryPrimaryKeys(): array
    {
        $sql = sprintf(
            'SHOW PRIMARY KEYS IN DATABASE %s',
            SnowflakeQuote::quoteSingleIdentifier($this->config->getDbDatabase()),
        );

        /** @var array<int, array{schema_name:string, table_name:string, column_name:string}> $primaryKeys */
        $primaryKeys = $this->connection->fetchAllAssociative($sql);

        $result = [];
        foreach ($primaryKeys as $primaryKey) {
            assert(is_array($primaryKey));
            $tableId = sprintf(
                '%s.%s',
                $primaryKey['schema_name'],
                $primaryKey['table_name'],
            );
            $result[$tableId][] = $primaryKey['column_name'];
        }

        return $result;
    }
}

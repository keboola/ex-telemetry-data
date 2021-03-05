<?php

declare(strict_types=1);

namespace Keboola\TelemetryData;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Exception\SnowflakeDbAdapterException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\TelemetryData\ValueObject\Column;
use Keboola\TelemetryData\ValueObject\Table;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use SplFileInfo;

class DbConnector
{
    private Config $config;

    private LoggerInterface $logger;

    private Connection $connection;

    private SplFileInfo $snowSqlConfigFile;

    public function __construct(Config $config, LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->config = $config;
        $this->connection = $this->createConnection();
        $this->snowSqlConfigFile = $this->createSnowSqlConfig();
    }

    private function createConnection(): Connection
    {
        $databaseConfigArray = [
            'host' => $this->config->getDbHost(),
            'user' => $this->config->getDbUser(),
            'password' => $this->config->getDbPassword(),
            'port' => $this->config->getDbPort(),
            'database' => $this->config->getDbDatabase(),
            'warehouse' => $this->config->getDbWarehouse(),
        ];

        try {
            $connection = new Connection($databaseConfigArray);
            $connection->query(
                sprintf(
                    'USE SCHEMA %s',
                    QueryBuilder::quoteIdentifier($this->config->getDbSchema())
                )
            );
        } catch (SnowflakeDbAdapterException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }

        return $connection;
    }

    public function getSnowSqlConfigFile(): SplFileInfo
    {
        return $this->snowSqlConfigFile;
    }

    public function getTables(?array $whiteList = null): array
    {
        if (is_array($whiteList) && !$whiteList) {
            return [];
        }

        $tables = $this->queryTables($whiteList);
        if (!$tables) {
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
                $tableObject->getName()
            );

            $tableObjects[$tableId] = $tableObject;
            $sqlWhereElements[] = sprintf(
                '(table_schema = %s AND table_name = %s)',
                QueryBuilder::quote($table['schema_name']),
                QueryBuilder::quote($table['name'])
            );
        }

        $primaryKeys = $this->queryPrimaryKeys();

        foreach ($this->queryColumns($sqlWhereElements) as $column) {
            $columnObject = Column::buildFromArray($column);
            $tableId = sprintf(
                '%s.%s',
                $columnObject->getTableSchema(),
                $columnObject->getTableName()
            );

            if (isset($primaryKeys[$tableId])) {
                $columnObject->setIsPrimaryKey(in_array($columnObject->getName(), $primaryKeys[$tableId]));
            }

            $tableObjects[$tableId]->addColumn($columnObject);
        }
        foreach ($tableObjects as $tableId => $table) {
            $missingColumns = $table->getMissingRequiredColumns();
            if ($missingColumns) {
                unset($tableObjects[$tableId]);
                $this->logger->info(sprintf(
                    'Missing "%s" columns for table "%s".',
                    implode(', ', $missingColumns),
                    $table->getName()
                ));
            }
        }

        return $tableObjects;
    }

    public function cleanupTableStage(string $tmpTableName): void
    {
        $sql = sprintf('REMOVE @~/%s;', $tmpTableName);
        $this->connection->query($sql);
    }

    public function fetchAll(string $sql): array
    {
        return $this->connection->fetchAll($sql);
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

    private function queryTables(?array $whiteList): array
    {
        $tables = $this->connection->fetchAll('SHOW TABLES IN SCHEMA');

        $filteredTables = array_filter($tables, function ($v) use ($whiteList) {
            return !$this->shouldTableBeSkipped($v, $whiteList);
        });

        usort($filteredTables, function ($item1, $item2) {
            return strnatcmp($item1['name'], $item2['name']);
        });

        return $filteredTables;
    }

    private function shouldTableBeSkipped(array $table, ?array $whiteList): bool
    {
        $isFromInformationSchema = $table['schema_name'] === 'INFORMATION_SCHEMA';
        $isStageTable = substr($table['name'], 0, 7) === 'staging';
        $isNotFromWhiteList = false;
        if ($whiteList) {
            $filteredWhiteList = array_filter($whiteList, function (Table $v) use ($table) {
                return $v->getSchema() === $table['schema_name'] && $v->getName() === $table['name'];
            });
            $isNotFromWhiteList = empty($filteredWhiteList);
        }
        return $isFromInformationSchema || $isNotFromWhiteList || $isStageTable;
    }

    private function queryColumns(array $queryTables): array
    {
        $sqlWhereClause = sprintf('WHERE %s', implode(' OR ', $queryTables));

        $sql = sprintf(
            'SELECT * FROM information_schema.columns %s ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION',
            $sqlWhereClause
        );

        return $this->connection->fetchAll($sql);
    }

    private function queryPrimaryKeys(): array
    {
        $sql = sprintf(
            'SHOW PRIMARY KEYS IN DATABASE %s',
            QueryBuilder::quoteIdentifier($this->config->getDbDatabase())
        );

        $primaryKeys = $this->connection->fetchAll($sql);

        $result = [];
        foreach ($primaryKeys as $primaryKey) {
            $tableId = sprintf(
                '%s.%s',
                $primaryKey['schema_name'],
                $primaryKey['table_name']
            );
            $result[$tableId][] = $primaryKey['column_name'];
        }

        return $result;
    }
}

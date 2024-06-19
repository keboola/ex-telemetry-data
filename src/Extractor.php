<?php

declare(strict_types=1);

namespace Keboola\TelemetryData;

use Keboola\Component\Manifest\ManifestManager;
use Keboola\Component\Manifest\ManifestManager\Options\OutTableManifestOptions;
use Keboola\Component\UserException;
use Keboola\Csv\CsvOptions;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\MySQL;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\TelemetryData\Exception\SnowsqlException;
use Keboola\TelemetryData\ValueObject\Column;
use Keboola\TelemetryData\ValueObject\Table;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;
use Throwable;

class Extractor
{
    /**
     * @param array<string, array{lastFetchedValue: string}> $inputState
     */
    public function __construct(
        private DbConnector $dbConnector,
        private Config $config,
        private LoggerInterface $logger,
        private ManifestManager $manifestManager,
        private string $datadir,
        private array $inputState,
    ) {
    }

    /**
     * @return array<string, array{lastFetchedValue: string}>
     */
    public function extractData(): array
    {
        $tableNamesForManifest = [];
        $result = [];

        /** @var Table $table */
        foreach ($this->dbConnector->getTables() as $table) {
            $this->logger->info(sprintf('Exporting to "%s"', $table->getName()));
            $retryProxy = $this->getRetryProxy();
            try {
                $rows = $retryProxy->call(fn(): int => $this->exportAndDownloadData($table));
            } catch (Throwable $e) {
                $message = sprintf('DB query failed: %s', $e->getMessage());
                throw new UserException($message, 0, $e);
            }

            if ($this->config->isIncrementalFetching($table->getName())) {
                $lastRow = $this->getLastRow($table);
                if ($lastRow) {
                    $result[$table->getName()] = [
                        Config::STATE_INCREMENTAL_KEY => $lastRow[Column::INCREMENTAL_NAME],
                    ];
                }
            }

            if ($rows > 0) {
                $tableNamesForManifest[] = $table;
            } else {
                $this->removeEmptyFile($table);
            }
        }
        $this->createManifestMetadata($tableNamesForManifest);
        return $result;
    }

    /**
     * @return array{
     *     dst_timestamp: string
     * }|null
     */
    private function getLastRow(Table $table): ?array
    {
        $sql = sprintf(
            'SELECT %s FROM %s.%s ORDER BY %s DESC LIMIT 1',
            QueryBuilder::quoteIdentifier(Column::INCREMENTAL_NAME),
            QueryBuilder::quoteIdentifier($table->getSchema()),
            QueryBuilder::quoteIdentifier($table->getName()),
            QueryBuilder::quoteIdentifier(Column::INCREMENTAL_NAME),
        );

        $resultLastRow = $this->getRetryProxy()->call(fn(): array => $this->dbConnector->fetchAll($sql));
        assert(
            is_array($resultLastRow) && count($resultLastRow) === 1,
            sprintf('getLastRow failed expected exactly one row from query "%s"', $sql),
        );

        return $resultLastRow[0] ?? null;
    }

    private function generateSqlStatement(Table $table): string
    {
        $sql = sprintf(
            'SELECT * FROM %s.%s',
            QueryBuilder::quoteIdentifier($table->getSchema()),
            QueryBuilder::quoteIdentifier($table->getName()),
        );

        $whereStatement = [];
        $orderStatement = '';
        switch ($this->config->getMode()) {
            case Config::MODE_PROJECT:
                $projectColumnName = Column::PROJECT_SINGLE_NAME;
                $stackColumnName = Column::STACK_SINGLE_NAME;
                break;
            case Config::MODE_ORGANIZATION:
                $projectColumnName = Column::PROJECT_COMPANY_NAME;
                $stackColumnName = Column::STACK_COMPANY_NAME;
                break;
            case Config::MODE_ACTIVITY_CENTER:
                $projectColumnName = Column::PROJECT_ACTIVITY_CENTER_NAME;
                $stackColumnName = Column::STACK_ACTIVITY_CENTER_NAME;
                break;
            default:
                throw new UserException(sprintf('Unknown mode "%s".', $this->config->getMode()));
        }

        $whereStatement[] = sprintf(
            '%s = %s',
            QueryBuilder::quoteIdentifier($projectColumnName),
            QueryBuilder::quote($this->config->getProjectId()),
        );
        $whereStatement[] = sprintf(
            '%s = %s',
            QueryBuilder::quoteIdentifier($stackColumnName),
            QueryBuilder::quote($this->config->getKbcStackId()),
        );

        if ($this->config->isIncrementalFetching($table->getName())) {
            if (isset($this->inputState[$table->getName()]['lastFetchedValue'])) {
                $whereStatement[] = sprintf(
                    '%s >= %s',
                    QueryBuilder::quoteIdentifier(Column::INCREMENTAL_NAME),
                    QueryBuilder::quote($this->inputState[$table->getName()]['lastFetchedValue']),
                );
            }
            $orderStatement = QueryBuilder::quoteIdentifier(Column::INCREMENTAL_NAME);
        }

        $sql .= sprintf(
            ' WHERE %s',
            implode(' AND ', $whereStatement),
        );

        if ($orderStatement !== '' && $orderStatement !== '0') {
            $sql .= ' ORDER BY ' . $orderStatement;
        }

        $this->logger->info(sprintf(
            'Run query "%s"',
            $sql,
        ));

        return $sql;
    }

    /**
     * @param Table[] $tableNames
     */
    private function createManifestMetadata(array $tableNames): void
    {
        $tableStructures = $this->dbConnector->getTables($tableNames);

        foreach ($tableStructures as $tableStructure) {
            $tableMetadata = [];
            $columnsMetadata = [];
            $columnNames = [];
            $primaryKeys = [];
            foreach ($tableStructure->getColumns() as $column) {
                $columnNames[] = $column->getName();
                try {
                    $datatype = new MySQL(
                        $column->getDataType(),
                        [
                            'length' => $column->getLength(),
                            'nullable' => $column->isNullable(),
                        ],
                    );
                } catch (InvalidTypeException) {
                    $datatype = new GenericStorage(
                        $column->getDataType(),
                        [
                            'nullable' => $column->isNullable(),
                        ],
                    );
                }
                $columnsMetadata[$column->getName()] = $datatype->toMetadata();
                if ($column->isPrimaryKey()) {
                    $primaryKeys[] = $column->getName();
                }
            }
            $tableMetadata[] = [
                'key' => 'KBC.name',
                'value' => $tableStructure->getName(),
            ];

            $tableManifestOptions = new OutTableManifestOptions();
            $tableManifestOptions
                ->setPrimaryKeyColumns($primaryKeys)
                ->setIncremental($this->config->isIncremental($tableStructure->getName()))
                ->setDestination($tableStructure->getName())
                ->setMetadata($tableMetadata)
                ->setColumns($columnNames)
                ->setColumnMetadata($columnsMetadata)
            ;
            $this->manifestManager->writeTableManifest(
                sprintf('%s.csv', $tableStructure->getName()),
                $tableManifestOptions,
            );
        }
    }

    private function exportAndDownloadData(Table $table): int
    {
        $tmpTableName = sprintf(
            '%s_%s_%s',
            $table->getName(),
            $this->config->getProjectId(),
            $this->config->getKbcStackId(),
        );
        $copyCommand = $this->generateCopyCommand(
            $tmpTableName,
            $this->generateSqlStatement($table),
        );

        $result = $this->dbConnector->fetchAll($copyCommand);
        $rowCount = (int) ($result[0]['rows_unloaded'] ?? 0);
        if ($rowCount === 0) {
            return 0;
        }

        $outputDataDir = $this->datadir . '/out/tables/' . $table->getName() . '.csv';
        if (!is_dir($outputDataDir)) {
            mkdir($outputDataDir, 0755, true);
        }

        $this->logger->info('Downloading data from Snowflake');

        $sqls = [];
        $sqls[] = sprintf('USE WAREHOUSE %s;', QueryBuilder::quoteIdentifier($this->config->getDbWarehouse()));
        $sqls[] = sprintf('USE DATABASE %s;', QueryBuilder::quoteIdentifier($this->config->getDbDatabase()));
        $sqls[] = sprintf(
            'USE SCHEMA %s.%s;',
            QueryBuilder::quoteIdentifier($this->config->getDbDatabase()),
            QueryBuilder::quoteIdentifier($this->config->getDbSchema()),
        );
        $sqls[] = sprintf(
            'GET @~/%s file://%s;',
            $tmpTableName,
            $outputDataDir,
        );

        $snowSqlFile = (new Temp())->createTmpFile('snowsql.sql');
        file_put_contents($snowSqlFile->getPathname(), implode("\n", $sqls));

        $command = sprintf(
            'snowsql --noup --config %s -c downloader -f %s',
            $this->dbConnector->getSnowSqlConfigFile(),
            $snowSqlFile,
        );

        $process = Process::fromShellCommandline($command);
        $process->setTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            $this->logger->error(sprintf('Snowsql error, process output %s', $process->getOutput()));
            $this->logger->error(sprintf('Snowsql error: %s', $process->getErrorOutput()));
            throw new SnowsqlException(sprintf(
                'File download error occurred processing [%s]',
                $table->getName(),
            ));
        }

        $this->dbConnector->cleanupTableStage($tmpTableName);

        return $rowCount;
    }

    private function ensureOutputTableDir(): string
    {
        $outputDir = implode(
            '/',
            [
                $this->datadir,
                'out',
                'tables',
            ],
        );
        $fs = new Filesystem();
        if (!$fs->exists($outputDir)) {
            $fs->mkdir($outputDir);
        }
        return $outputDir;
    }

    private function removeEmptyFile(Table $table): void
    {
        $filename = sprintf(
            '%s/%s.csv',
            $this->ensureOutputTableDir(),
            $table->getName(),
        );

        $fs = new Filesystem();
        if ($fs->exists($filename)) {
            $fs->remove($filename);
        }
    }

    private function generateCopyCommand(string $stageTmpPath, string $query): string
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', QueryBuilder::quote(CsvOptions::DEFAULT_DELIMITER));
        $csvOptions[] = sprintf(
            'FIELD_OPTIONALLY_ENCLOSED_BY = %s',
            QueryBuilder::quote(CsvOptions::DEFAULT_ENCLOSURE),
        );
        $csvOptions[] = sprintf('ESCAPE_UNENCLOSED_FIELD = %s', QueryBuilder::quote('\\'));
        $csvOptions[] = sprintf('COMPRESSION = %s', QueryBuilder::quote('GZIP'));
        $csvOptions[] = 'NULL_IF=()';

        return sprintf(
            '
            COPY INTO @~/%s/part
            FROM (%s)

            FILE_FORMAT = (TYPE=CSV %s)
            HEADER = false
            MAX_FILE_SIZE=50000000
            OVERWRITE = TRUE
            ;
            ',
            $stageTmpPath,
            rtrim(trim($query), ';'),
            implode(' ', $csvOptions),
        );
    }

    private function getRetryProxy(): RetryProxy
    {
        return new RetryProxy(
            new SimpleRetryPolicy(
                Config::RETRY_MAX_ATTEMPTS,
                ['Exception', 'SnowsqlException'],
            ),
            new ExponentialBackOffPolicy(Config::RETRY_DEFAULT_BACKOFF_INTERVAL),
            $this->logger,
        );
    }
}

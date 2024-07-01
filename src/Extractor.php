<?php

declare(strict_types=1);

namespace Keboola\TelemetryData;

use Keboola\Component\Manifest\ManifestManager;
use Keboola\Component\Manifest\ManifestManager\Options\OutTable\ManifestOptionsSchema;
use Keboola\Component\UserException;
use Keboola\Csv\CsvOptions;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\BuilderHelper;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TelemetryData\Exception\SnowsqlException;
use Keboola\TelemetryData\ValueObject\Column;
use Keboola\TelemetryData\ValueObject\Table;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use RuntimeException;
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
                        Config::STATE_INCREMENTAL_KEY => $lastRow,
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

    private function getLastRow(Table $table): ?string
    {
        $sql = sprintf(
            'SELECT %s FROM %s.%s ORDER BY %s DESC LIMIT 1',
            SnowflakeQuote::quoteSingleIdentifier(Column::INCREMENTAL_NAME),
            SnowflakeQuote::quoteSingleIdentifier($table->getSchema()),
            SnowflakeQuote::quoteSingleIdentifier($table->getName()),
            SnowflakeQuote::quoteSingleIdentifier(Column::INCREMENTAL_NAME),
        );

        $resultLastRow = $this->getRetryProxy()->call(
            fn(): string|null => $this->dbConnector->fetchOneStringOrNull($sql),
        );
        assert($resultLastRow === null || is_string($resultLastRow));

        return $resultLastRow;
    }

    private function generateSqlStatement(Table $table): string
    {
        $sql = sprintf(
            'SELECT * FROM %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($table->getSchema()),
            SnowflakeQuote::quoteSingleIdentifier($table->getName()),
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
            SnowflakeQuote::quoteSingleIdentifier($projectColumnName),
            SnowflakeQuote::quote($this->config->getProjectId()),
        );
        $whereStatement[] = sprintf(
            '%s = %s',
            SnowflakeQuote::quoteSingleIdentifier($stackColumnName),
            SnowflakeQuote::quote($this->config->getKbcStackId()),
        );

        if ($this->config->isIncrementalFetching($table->getName())) {
            if (isset($this->inputState[$table->getName()]['lastFetchedValue'])) {
                $whereStatement[] = sprintf(
                    '%s >= %s',
                    SnowflakeQuote::quoteSingleIdentifier(Column::INCREMENTAL_NAME),
                    SnowflakeQuote::quote($this->inputState[$table->getName()]['lastFetchedValue']),
                );
            }
            $orderStatement = SnowflakeQuote::quoteSingleIdentifier(Column::INCREMENTAL_NAME);
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
            $schema = [];
            foreach ($tableStructure->getColumns() as $column) {
                $schema[] = $this->createManifestOptionsSchema($column);
            }
            $tableMetadata = [
                'KBC.name' => $tableStructure->getName(),
                'KBC.sanitizedName' => BuilderHelper::sanitizeName($tableStructure->getName()),
                'KBC.datatype.backend' => 'snowflake',
            ];

            $options = new ManifestManager\Options\OutTable\ManifestOptions();
            $options
                ->setSchema($schema)
                ->setIncremental($this->config->isIncremental($tableStructure->getName()))
                ->setDestination($tableStructure->getName())
                ->setTableMetadata($tableMetadata);

            $this->manifestManager->writeTableManifest(
                sprintf('%s.csv', $tableStructure->getName()),
                $options,
                $this->config->getDataTypeSupport()->usingLegacyManifest(),
            );
        }
    }

    private function createManifestOptionsSchema(
        Column $column,
    ): ManifestOptionsSchema {
        try {
            $datatype = new Snowflake(
                $column->getDataType(),
                [
                    'length' => $column->getLength(),
                    'nullable' => $column->isNullable(),
                ],
            );
        } catch (InvalidTypeException|InvalidLengthException) {
            $datatype = new GenericStorage(
                $column->getDataType(),
                [
                    'nullable' => $column->isNullable(),
                ],
            );
        }

        $columnMetadata = [];
        foreach ($datatype->toMetadata() as $item) {
            if ($item['value'] !== null) {
                $columnMetadata[$item['key']] = $item['value'];
            }
        }

        $dataTypes = [
            'base' => array_filter([
                'type' => $datatype->getBasetype(),
                'length' => $datatype->getLength(),
                'default' => $datatype->getDefault(),
            ], fn($value) => $value !== null),
        ];

        if ($datatype instanceof Snowflake) {
            $dataTypes['snowflake'] = array_filter([
                'type' => $datatype->getType(),
                'length' => $datatype->getLength(),
                'default' => $datatype->getDefault(),
            ], fn($value) => $value !== null);
        }

        return new ManifestOptionsSchema(
            BuilderHelper::sanitizeName($column->getName()),
            $dataTypes,
            $datatype->isNullable(),
            $column->isPrimaryKey(),
            null,
            $columnMetadata,
        );
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
        if (!is_dir($outputDataDir) && !mkdir($outputDataDir, 0755, true) && !is_dir($outputDataDir)) {
            throw new RuntimeException(sprintf('Directory "%s" was not created', $outputDataDir));
        }

        $this->logger->info('Downloading data from Snowflake');

        $sqls = [];
        $sqls[] = sprintf('USE WAREHOUSE %s;', SnowflakeQuote::quoteSingleIdentifier($this->config->getDbWarehouse()));
        $sqls[] = sprintf('USE DATABASE %s;', SnowflakeQuote::quoteSingleIdentifier($this->config->getDbDatabase()));
        $sqls[] = sprintf(
            'USE SCHEMA %s.%s;',
            SnowflakeQuote::quoteSingleIdentifier($this->config->getDbDatabase()),
            SnowflakeQuote::quoteSingleIdentifier($this->config->getDbSchema()),
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
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', SnowflakeQuote::quote(CsvOptions::DEFAULT_DELIMITER));
        $csvOptions[] = sprintf(
            'FIELD_OPTIONALLY_ENCLOSED_BY = %s',
            SnowflakeQuote::quote(CsvOptions::DEFAULT_ENCLOSURE),
        );
        $csvOptions[] = sprintf('ESCAPE_UNENCLOSED_FIELD = %s', SnowflakeQuote::quote('\\'));
        $csvOptions[] = sprintf('COMPRESSION = %s', SnowflakeQuote::quote('GZIP'));
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

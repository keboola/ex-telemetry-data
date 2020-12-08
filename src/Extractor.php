<?php

declare(strict_types=1);

namespace Keboola\TelemetryData;

use Keboola\Component\Manifest\ManifestManager;
use Keboola\Component\UserException;
use Keboola\Csv\CsvWriter;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\MySQL;
use Keboola\TelemetryData\ValueObject\Column;
use Keboola\TelemetryData\ValueObject\Table;
use Psr\Log\LoggerInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use \PDOException;
use \PDOStatement;
use \PDO;
use Symfony\Component\Filesystem\Filesystem;
use Keboola\Component\Manifest\ManifestManager\Options\OutTableManifestOptions;

class Extractor
{

    private DbConnector $dbConnector;

    private Config $config;

    private LoggerInterface $logger;

    private ManifestManager $manifestManager;

    private string $datadir;

    public function __construct(
        DbConnector $dbConnector,
        Config $config,
        LoggerInterface $logger,
        ManifestManager $manifestManager,
        string $datadir
    ) {
        $this->dbConnector = $dbConnector;
        $this->config = $config;
        $this->logger = $logger;
        $this->datadir = $datadir;
        $this->manifestManager = $manifestManager;
    }

    public function extractData(): void
    {
        $tableNamesForManifest = [];

        /** @var Table $table */
        foreach ($this->dbConnector->getTables() as $table) {
            $retryProxy = new RetryProxy(
                new SimpleRetryPolicy(
                    Config::RETRY_MAX_ATTEMPTS,
                    ['Exception', 'ErrorExceptions', 'PDOException']
                ),
                new ExponentialBackOffPolicy(Config::RETRY_DEFAULT_BACKOFF_INTERVAL),
                $this->logger
            );

            try {
                $result = $retryProxy->call(function () use ($table) {
                    $stmt = $this->dbConnector->execute(
                        $this->generateSqlStatement($table)
                    );
                    $csv = $this->createOutputCsvFile($table);
                    return $this->writeDataToCsv($stmt, $csv);
                });
            } catch (PDOException $e) {
                $message = sprintf('DB query failed: %s', $e->getMessage());
                throw new UserException($message, 0, $e);
            }

            if ($result['rows'] > 0) {
                $tableNamesForManifest[] = $table;
            } else {
                $this->removeEmptyFile($table);
            }
        }

        $this->createManifestMetadata($tableNamesForManifest);
    }

    private function generateSqlStatement(Table $table): string
    {
        $sql = sprintf(
            'SELECT * FROM %s.%s',
            $this->dbConnector->quoteIdentifier($table->getSchema()),
            $this->dbConnector->quoteIdentifier($table->getName())
        );

        $whereStatement = [];
        switch ($this->config->getMode()) {
            case Config::MODE_PROJECT:
                $projectColumnName = Column::PROJECT_SINGLE_NAME;
                $stackColumnName = Column::STACK_SINGLE_NAME;
                break;
            case Config::MODE_ORGANIZATION:
                $projectColumnName = Column::PROJECT_COMPANY_NAME;
                $stackColumnName = Column::STACK_COMPANY_NAME;
                break;
            default:
                throw new UserException(sprintf('Unknown mode "%s".', $this->config->getMode()));
        }

        $whereStatement[] = sprintf(
            '%s = %s',
            $this->dbConnector->quoteIdentifier($projectColumnName),
            $this->dbConnector->quote($this->config->getProjectId())
        );
        $whereStatement[] = sprintf(
            '%s = %s',
            $this->dbConnector->quoteIdentifier($stackColumnName),
            $this->dbConnector->quote($this->config->getKbcStackId())
        );

        $sql .= sprintf(
            ' WHERE %s',
            implode(' AND ', $whereStatement)
        );

        return $sql;
    }

    private function createManifestMetadata(array $tableNames): void
    {
        /** @var Table[] $tableStructures */
        $tableStructures = $this->dbConnector->getTables($tableNames);

        foreach ($tableStructures as $tableStructure) {
            $tableMetadata = [];
            $columnsMetadata = [];
            $columnNames = [];
            foreach ($tableStructure->getColumns() as $column) {
                $columnNames[] = $column->getName();
                try {
                    $datatype = new MySQL(
                        $column->getDataType(),
                        [
                            'length' => $column->getLength(),
                            'nullable' => $column->isNullable(),
                        ]
                    );
                } catch (InvalidTypeException $e) {
                    $datatype = new GenericStorage(
                        $column->getDataType(),
                        [
                            'nullable' => $column->isNullable(),
                        ]
                    );
                }
                $columnsMetadata[$column->getName()] = $datatype->toMetadata();
            }
            $tableMetadata[] = [
                'key' => 'KBC.name',
                'value' => $tableStructure->getName(),
            ];

            $tableManifestOptions = new OutTableManifestOptions();
            $tableManifestOptions
                ->setMetadata($tableMetadata)
                ->setColumns($columnNames)
                ->setColumnMetadata($columnsMetadata)
            ;
            $this->manifestManager->writeTableManifest(
                sprintf('%s.csv', $tableStructure->getName()),
                $tableManifestOptions
            );
        }
    }

    private function writeDataToCsv(PDOStatement $statement, CsvWriter $csvWriter): array
    {
        $result = [
            'rows' => 0,
        ];
        while ($row = $statement->fetch(PDO::FETCH_ASSOC)) {
            $csvWriter->writeRow($row);
            $result['rows']++;
        }
        return $result;
    }

    private function ensureOutputTableDir(): string
    {
        $outputDir = implode(
            '/',
            [
                $this->datadir,
                'out',
                'tables',
            ]
        );
        $fs = new Filesystem();
        if (!$fs->exists($outputDir)) {
            $fs->mkdir($outputDir);
        }
        return $outputDir;
    }

    private function createOutputCsvFile(Table $table): CsvWriter
    {
        $filename = sprintf(
            '%s/%s.csv',
            $this->ensureOutputTableDir(),
            $table->getName()
        );

        return new CsvWriter($filename);
    }

    private function removeEmptyFile(Table $table): void
    {
        $filename = sprintf(
            '%s/%s.csv',
            $this->ensureOutputTableDir(),
            $table->getName()
        );

        $fs = new Filesystem();
        if ($fs->exists($filename)) {
            $fs->remove($filename);
        }
    }
}

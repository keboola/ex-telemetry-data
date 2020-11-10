<?php

declare(strict_types=1);

namespace Keboola\TelemetryData;

use Keboola\Component\Logger;
use Keboola\Component\Manifest\ManifestManager;
use Keboola\Component\UserException;
use Keboola\Csv\CsvWriter;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\MySQL;
use Keboola\TelemetryData\ValueObject\Column;
use Keboola\TelemetryData\ValueObject\Table;
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

    private Logger $logger;

    private ManifestManager $manifestManager;

    private string $datadir;

    public function __construct(
        DbConnector $dbConnector,
        Config $config,
        Logger $logger,
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
            $sql = sprintf(
                'SELECT * FROM %s.%s',
                $this->dbConnector->quoteIdentifier($table->getSchema()),
                $this->dbConnector->quoteIdentifier($table->getName())
            );

            $whereStatement = [];
            if ($table->hasProjectStackColumn()) {
                $whereStatement[] = sprintf(
                    '%s = %s',
                    $this->dbConnector->quoteIdentifier(Column::PROJECT_STACK_NAME),
                    $this->dbConnector->quote($this->config->getKbcStackId())
                );
            }

            if ($table->hasProjectIdColumn()) {
                $whereStatement[] = sprintf(
                    '%s = %s',
                    $this->dbConnector->quoteIdentifier(Column::PROJECT_ID_NAME),
                    $this->dbConnector->quote($this->config->getProjectId())
                );
            }

            if ($whereStatement) {
                $sql .= sprintf(
                    ' WHERE %s',
                    implode(' AND ', $whereStatement)
                );
            }

            $retryProxy = new RetryProxy(
                new SimpleRetryPolicy(
                    Config::RETRY_MAX_ATTEMPTS,
                    ['Exception', 'ErrorExceptions', 'PDOException']
                ),
                new ExponentialBackOffPolicy(Config::RETRY_DEFAULT_BACKOFF_INTERVAL),
                $this->logger
            );

            try {
                $this->logger->info(sprintf('Run query "%s"', $sql));
                $result = $retryProxy->call(function () use ($sql, $table) {
                    $stmt = $this->dbConnector->execute($sql);
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

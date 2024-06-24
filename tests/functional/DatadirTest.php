<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\FunctionalTests;

use Doctrine\DBAL\Connection;
use Keboola\Csv\CsvReader;
use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecificationInterface;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;
use Throwable;

class DatadirTest extends DatadirTestCase
{
    private Connection $connection;

    public function setUp(): void
    {
        $this->connection = SnowflakeConnectionFactory::getConnection(
            (string) getenv('SNOWFLAKE_DB_HOST'),
            (string) getenv('SNOWFLAKE_DB_USER'),
            (string) getenv('SNOWFLAKE_DB_PASSWORD'),
            [
                'port' => (string) getenv('SNOWFLAKE_DB_PORT'),
                'warehouse' => (string) getenv('SNOWFLAKE_DB_WAREHOUSE'),
                'database' => (string) getenv('SNOWFLAKE_DB_DATABASE'),
            ],
        );
        $this->connection->executeStatement(
            sprintf(
                'USE SCHEMA %s',
                SnowflakeQuote::quoteSingleIdentifier((string) getenv('SNOWFLAKE_DB_SCHEMA')),
            ),
        );
        parent::setUp();
    }

    /**
     * @dataProvider provideDatadirSpecifications
     */
    public function testDatadir(DatadirTestSpecificationInterface $specification): void
    {
        $tempDatadir = $this->getTempDatadir($specification);

        $this->cleanDatabase();
        $this->initDatabase((string) $specification->getSourceDatadirDirectory());

        $process = $this->runScript($tempDatadir->getTmpFolder());

        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
    }

    public function assertDirectoryContentsSame(string $expected, string $actual): void
    {
        $this->prettifyAllManifests($actual);
        $this->ungzipFiles($actual);
        parent::assertDirectoryContentsSame($expected, $actual);
    }

    private function cleanDatabase(): void
    {
        $sql = 'SHOW TABLES IN SCHEMA';

        $items = $this->connection->fetchAllAssociative($sql);
        /** @var array{schema_name:string,name:string} $item */
        foreach ($items as $item) {
            $this->connection->executeQuery(
                sprintf(
                    'DROP TABLE IF EXISTS %s.%s',
                    SnowflakeQuote::quoteSingleIdentifier($item['schema_name']),
                    SnowflakeQuote::quoteSingleIdentifier($item['name']),
                ),
            );
        }
    }

    private function initDatabase(string $datadirDirectory): void
    {
        $finder = new Finder();
        $importFiles = $finder->name('*.manifest')->in($datadirDirectory . '/in/tables');

        foreach ($importFiles as $file) {
            $databaseTableName = substr(
                $file->getFilename(),
                0,
                (int) strpos($file->getFilename(), '.csv.manifest'),
            );

            $fileContent = json_decode(
                (string) file_get_contents($file->getPathname()),
                true,
                512,
                JSON_THROW_ON_ERROR,
            );
            assert(is_array($fileContent));

            $columns = [];
            foreach ($fileContent['columns'] as $column) {
                $columns[] = sprintf(
                    '%s VARCHAR(512)',
                    SnowflakeQuote::quoteSingleIdentifier($column),
                );
            }

            $sqlTemplate = <<<SQL
CREATE TABLE %s (
    %s
);
SQL;
            $sql = sprintf(
                $sqlTemplate,
                SnowflakeQuote::quoteSingleIdentifier($databaseTableName),
                implode(',', $columns),
            );

            $this->connection->executeQuery($sql);

            if (array_key_exists('primary_key', $fileContent) && is_array($fileContent['primary_key'])) {
                $sqlConstraintsTemplate = <<<SQL
ALTER TABLE %s ADD CONSTRAINT PK_%s PRIMARY KEY (%s)
SQL;

                $sql = sprintf(
                    $sqlConstraintsTemplate,
                    SnowflakeQuote::quoteSingleIdentifier($databaseTableName),
                    $databaseTableName,
                    implode(
                        ', ',
                        array_map(
                            fn(string $v): string => SnowflakeQuote::quoteSingleIdentifier($v),
                            $fileContent['primary_key'],
                        ),
                    ),
                );

                $this->connection->executeQuery($sql);
            }

            $sqlInsertTemplate = <<<SQL
INSERT INTO %s VALUES (%s);
SQL;

            $csvReader = new CsvReader(substr(
                $file->getPathname(),
                0,
                (int) strpos($file->getPathname(), '.manifest'),
            ));

            while ($csvReader->current()) {
                $current = $csvReader->current();
                assert(is_array($current));
                $row = array_map(
                    fn($item): string => SnowflakeQuote::quote($item),
                    $current,
                );

                $sqlInsert = sprintf(
                    $sqlInsertTemplate,
                    SnowflakeQuote::quoteSingleIdentifier($databaseTableName),
                    implode(', ', $row),
                );

                $this->connection->executeQuery($sqlInsert);
                $csvReader->next();
            }
        }
    }

    protected function prettifyAllManifests(string $actual): void
    {
        foreach ($this->findManifests($actual . '/tables') as $file) {
            $this->prettifyJsonFile((string) $file->getRealPath());
        }
    }

    protected function prettifyJsonFile(string $path): void
    {
        $json = (string) file_get_contents($path);
        try {
            file_put_contents($path, (string) json_encode(json_decode($json), JSON_PRETTY_PRINT));
        } catch (Throwable) {
            // If a problem occurs, preserve the original contents
            file_put_contents($path, $json);
        }
    }

    protected function findManifests(string $dir): Finder
    {
        $finder = new Finder();
        return $finder->files()->in($dir)->name(['~.*\.manifest~']);
    }

    protected function ungzipFiles(string $actualDir): void
    {
        $fs = new Filesystem();
        if (!$fs->exists($actualDir . '/tables')) {
            return;
        }
        $gzipFiles = $this->findGzipFiles($actualDir . '/tables');
        foreach ($gzipFiles as $gzipFile) {
            $process = Process::fromShellCommandline('gzip -d ' . $gzipFile->getRealPath());
            $process->run();
        }
    }

    private function findGzipFiles(string $dir): Finder
    {
        $finder = new Finder();
        return $finder->files()->in($dir)->depth(1)->name(['~.*\.csv.gz$~']);
    }
}

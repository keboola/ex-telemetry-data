<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\FunctionalTests;

use Keboola\Csv\CsvReader;
use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecificationInterface;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;
use Throwable;

class DatadirTest extends DatadirTestCase
{
    private Connection $connection;

    public function setUp(): void
    {
        $this->markTestSkipped();
        $this->connection = new Connection(
            [
                'host' => getenv('SNOWFLAKE_DB_HOST'),
                'user' => getenv('SNOWFLAKE_DB_USER'),
                'password' => getenv('SNOWFLAKE_DB_PASSWORD'),
                'port' => getenv('SNOWFLAKE_DB_PORT'),
                'database' => getenv('SNOWFLAKE_DB_DATABASE'),
                'warehouse' => getenv('SNOWFLAKE_DB_WAREHOUSE'),
            ]
        );
        $this->connection->query(
            sprintf(
                'USE SCHEMA %s',
                QueryBuilder::quoteIdentifier((string) getenv('SNOWFLAKE_DB_SCHEMA'))
            )
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

        $items = $this->connection->fetchAll($sql);
        foreach ($items as $item) {
            $this->connection->query(
                sprintf(
                    'DROP TABLE IF EXISTS %s.%s',
                    QueryBuilder::quoteIdentifier($item['schema_name']),
                    QueryBuilder::quoteIdentifier($item['name'])
                )
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
                (int) strpos($file->getFilename(), '.csv.manifest')
            );

            $fileContent = json_decode(
                (string) file_get_contents($file->getPathname()),
                true
            );

            $columns = [];
            foreach ($fileContent['columns'] as $column) {
                $columns[] = sprintf(
                    '%s VARCHAR(512)',
                    QueryBuilder::quoteIdentifier($column)
                );
            }

            $sqlTemplate = <<<SQL
CREATE TABLE %s (
    %s
);
SQL;
            $sql = sprintf(
                $sqlTemplate,
                QueryBuilder::quoteIdentifier($databaseTableName),
                implode(',', $columns)
            );

            $this->connection->query($sql);

            if (isset($fileContent['primary_key'])) {
                $sqlConstraintsTemplate = <<<SQL
ALTER TABLE %s ADD CONSTRAINT PK_%s PRIMARY KEY (%s)
SQL;

                $sql = sprintf(
                    $sqlConstraintsTemplate,
                    QueryBuilder::quoteIdentifier($databaseTableName),
                    $databaseTableName,
                    implode(
                        ', ',
                        array_map(fn(string $v) => QueryBuilder::quoteIdentifier($v), $fileContent['primary_key'])
                    )
                );

                $this->connection->query($sql);
            }

            $sqlInsertTemplate = <<<SQL
INSERT INTO %s VALUES (%s);
SQL;

            $csvReader = new CsvReader(substr(
                $file->getPathname(),
                0,
                (int) strpos($file->getPathname(), '.manifest')
            ));

            while ($csvReader->current()) {
                $row = array_map(function ($item) {
                    return QueryBuilder::quote($item);
                }, $csvReader->current());

                $sqlInsert = sprintf(
                    $sqlInsertTemplate,
                    QueryBuilder::quoteIdentifier($databaseTableName),
                    implode(', ', $row)
                );

                $this->connection->query($sqlInsert);
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
        } catch (Throwable $e) {
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

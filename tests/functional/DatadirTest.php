<?php

declare(strict_types=1);

namespace Keboola\TelemetryData\FunctionalTests;

use Keboola\Csv\CsvReader;
use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecificationInterface;
use Keboola\TelemetryData\Exception\ApplicationException;
use Symfony\Component\Finder\Finder;
use \PDO;

class DatadirTest extends DatadirTestCase
{
    private PDO $connection;

    public function setUp(): void
    {
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, // convert errors to PDOExceptions
        ];

        $this->connection = new PDO(
            sprintf(
                'mysql:host=%s;port=%s;dbname=%s;charset=utf8',
                getenv('MYSQL_DB_HOST'),
                getenv('MYSQL_DB_PORT'),
                getenv('MYSQL_DB_DATABASE')
            ),
            (string) getenv('MYSQL_DB_USER'),
            (string) getenv('MYSQL_DB_PASSWORD'),
            $options
        );

        $this->connection->exec('SET NAMES utf8mb4;');
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

    private function cleanDatabase(): void
    {
        $whereStatement = [
            sprintf(
                'LOWER(`TABLE_SCHEMA`) = \'%s\'',
                getenv('MYSQL_DB_DATABASE')
            ),
        ];

        $sql = sprintf(
            'SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE %s',
            implode(' AND ', $whereStatement)
        );

        $stmt = $this->connection->query($sql);
        if (!$stmt) {
            throw new ApplicationException('Get tables failed.');
        }

        $items = (array) $stmt->fetchAll(PDO::FETCH_ASSOC);
        foreach ($items as $item) {
            $this->connection->query(
                sprintf(
                    'DROP TABLE IF EXISTS `%s`.`%s`',
                    $item['TABLE_SCHEMA'],
                    $item['TABLE_NAME']
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
                    '`%s` VARCHAR(512)',
                    $column
                );
            }

            $sqlTemplate = <<<SQL
CREATE TABLE `%s` (
    %s
);
SQL;
            $sql = sprintf(
                $sqlTemplate,
                $databaseTableName,
                implode(',', $columns)
            );

            $this->connection->query($sql);

            $sqlInsertTemplate = <<<SQL
INSERT INTO `%s` VALUES (%s);
SQL;

            $csvReader = new CsvReader(substr(
                $file->getPathname(),
                0,
                (int) strpos($file->getPathname(), '.manifest')
            ));

            while ($csvReader->current()) {
                $row = array_map(function ($item) {
                    return $this->connection->quote($item);
                }, $csvReader->current());

                $sqlInsert = sprintf(
                    $sqlInsertTemplate,
                    $databaseTableName,
                    implode(', ', $row)
                );

                $this->connection->query($sqlInsert);
                $csvReader->next();
            }
        }
    }
}

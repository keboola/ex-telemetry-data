<?php

declare(strict_types=1);

namespace Keboola\TelemetryData;

use Keboola\TelemetryData\Exception\ApplicationException;
use Keboola\TelemetryData\ValueObject\Column;
use Keboola\TelemetryData\ValueObject\Table;
use \PDO;
use \PDOException;
use \PDOStatement;
use Psr\Log\LoggerInterface;

class DbConnector
{
    private Config $config;

    private LoggerInterface $logger;

    private PDO $connection;

    public function __construct(Config $config, LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->config = $config;
        $this->connection = $this->createConnection();
    }

    private function createConnection(): PDO
    {
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, // convert errors to PDOExceptions
        ];

        $dsn = sprintf(
            'mysql:host=%s;port=%s;dbname=%s;charset=utf8',
            $this->config->getDbHost(),
            $this->config->getDbPort(),
            $this->config->getDbDatabase()
        );

        $this->logger->info(sprintf('Connecting to DSN %s', $dsn));

        try {
            $pdo = new PDO($dsn, $this->config->getDbUser(), $this->config->getDbPassword(), $options);
        } catch (PDOException $e) {
            // SQLSTATE[HY000] is general error without message, so throw previous exception
            if (strpos($e->getMessage(), 'SQLSTATE[HY000]') === 0 && $e->getPrevious() !== null) {
                throw $e->getPrevious();
            }

            throw $e;
        }

        try {
            $pdo->exec('SET NAMES utf8mb4;');
        } catch (PDOException $exception) {
            $this->logger->info('Falling back to "utf8" charset');
            $pdo->exec('SET NAMES utf8;');
        }

        return $pdo;
    }

    public function fetchAll(string $sql): array
    {
        $stmt = $this->connection->query($sql);
        if (!$stmt) {
            throw new ApplicationException(sprintf('Query execution failed: %s', $sql));
        }
        return (array) $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    public function getTables(?array $sourceTables = null): array
    {
        if (is_array($sourceTables) && !$sourceTables) {
            return [];
        }
        $whereStatement = [
            sprintf(
                'LOWER(%s) = %s',
                $this->quoteIdentifier('TABLE_SCHEMA'),
                $this->quote(
                    $this->config->getDbDatabase()
                )
            ),
        ];

        if ($sourceTables) {
            $sourceTablesQuote = array_map(function (Table $v) {
                return $this->quote($v->getName());
            }, $sourceTables);
            $whereStatement[] = sprintf(
                '%s IN (%s)',
                $this->quoteIdentifier('TABLE_NAME'),
                implode(', ', $sourceTablesQuote)
            );
        }

        $sqlColumns = sprintf(
            'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE %s ORDER BY %s',
            implode(' AND ', $whereStatement),
            $this->quoteIdentifier('ORDINAL_POSITION')
        );

        $sqlTables = sprintf(
            'SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE %s',
            implode(' AND ', $whereStatement)
        );

        $tables = [];
        foreach ($this->fetchAll($sqlTables) as $table) {
            $tableObject = Table::buildFromArray(
                $table,
            );

            $tableId = sprintf(
                '%s.%s',
                $tableObject->getSchema(),
                $tableObject->getName()
            );

            $tables[$tableId] = $tableObject;
        }

        foreach ($this->fetchAll($sqlColumns) as $column) {
            $columnObject = Column::buildFromArray($column);
            $tableId = sprintf(
                '%s.%s',
                $columnObject->getTableSchema(),
                $columnObject->getTableName()
            );

            $tables[$tableId]->addColumn($columnObject);
        }
        return $tables;
    }

    public function quote(string $str): string
    {
        return $this->connection->quote($str);
    }

    public function quoteIdentifier(string $str): string
    {
        return sprintf('`%s`', $str);
    }

    public function execute(string $sql): PDOStatement
    {
        $stmt = $this->connection->prepare($sql);
        $stmt->execute();
        return $stmt;
    }
}

<?php

declare(strict_types=1);

namespace Keboola\TelemetryData;

use Keboola\Component\Config\BaseConfig;
use Keboola\Component\UserException;

class Config extends BaseConfig
{
    public const RETRY_MAX_ATTEMPTS = 5;

    public const RETRY_DEFAULT_BACKOFF_INTERVAL = 1000;

    public const MODE_PROJECT = 'project';

    public const MODE_ORGANIZATION = 'organization';

    public const STATE_INCREMENTAL_KEY = 'lastFetchedValue';

    public function getProjectId(): string
    {
        $projectId = getenv('KBC_PROJECTID');
        if (!$projectId) {
            throw new UserException('Cannot find "KBC_PROJECTID" environment.');
        }
        return $projectId;
    }

    public function getMode(): string
    {
        return $this->getValue(['parameters', 'mode']);
    }

    public function getKbcStackId(): string
    {
        $url = getenv('KBC_STACKID');
        if (!$url) {
            throw new UserException('Cannot find "KBC_STACKID" environment.');
        }
        return $url;
    }

    public function getDbHost(): string
    {
        $imageParameters = $this->getImageParameters();
        return $imageParameters['db']['host'];
    }

    public function getDbPort(): string
    {
        $imageParameters = $this->getImageParameters();
        return (string) $imageParameters['db']['port'];
    }

    public function getDbUser(): string
    {
        $imageParameters = $this->getImageParameters();
        return $imageParameters['db']['user'];
    }

    public function getDbPassword(): string
    {
        $imageParameters = $this->getImageParameters();
        return $imageParameters['db']['#password'];
    }

    public function getDbDatabase(): string
    {
        $imageParameters = $this->getImageParameters();
        return $imageParameters['db']['database'];
    }

    public function getDbSchema(): string
    {
        $imageParameters = $this->getImageParameters();
        return $imageParameters['db']['schema'];
    }

    public function getDbWarehouse(): string
    {
        $imageParameters = $this->getImageParameters();
        return $imageParameters['db']['warehouse'];
    }

    public function isIncrementalFetching(): bool
    {
        return (bool) $this->getValue(['parameters', 'incrementalFetching']);
    }

    public function isIncremental(): bool
    {
        return (bool) $this->getValue(['parameters', 'incremental']);
    }
}

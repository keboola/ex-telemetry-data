<?php

declare(strict_types=1);

namespace Keboola\TelemetryData;

use Keboola\Component\BaseComponent;
use Keboola\Component\Logger;
use Keboola\Component\Manifest\ManifestManager;

class Component extends BaseComponent
{
    protected function run(): void
    {
        $extractor = new Extractor(
            new DbConnector($this->getAppConfig(), $this->getAppLogger()),
            $this->getAppConfig(),
            $this->getAppLogger(),
            $this->getManifestManager(),
            $this->getDataDir()
        );

        $extractor->extractData();
    }

    private function getAppConfig(): Config
    {
        /** @var Config $config */
        $config = $this->getConfig();

        return $config;
    }

    private function getAppLogger(): Logger
    {
        /** @var Logger $logger */
        $logger = $this->getLogger();
        return $logger;
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        return ConfigDefinition::class;
    }
}

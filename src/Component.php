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
            new DbConnector($this->getAppConfig(), $this->getLogger()),
            $this->getAppConfig(),
            $this->getLogger(),
            $this->getManifestManager(),
            $this->getDataDir(),
            $this->getInputState()
        );

        $result = $extractor->extractData();

        if (isset($result[Config::STATE_INCREMENTAL_KEY])) {
            $this->writeOutputStateToFile(
                [Config::STATE_INCREMENTAL_KEY => $result[Config::STATE_INCREMENTAL_KEY]]
            );
        } elseif (isset($this->getInputState()[Config::STATE_INCREMENTAL_KEY])) {
            $this->writeOutputStateToFile($this->getInputState());
        }
    }

    private function getAppConfig(): Config
    {
        /** @var Config $config */
        $config = $this->getConfig();

        return $config;
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

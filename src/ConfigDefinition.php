<?php

declare(strict_types=1);

namespace Keboola\TelemetryData;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->children()
                ->enumNode('mode')
                    ->values([Config::MODE_PROJECT, Config::MODE_ORGANIZATION])
                    ->defaultValue(Config::MODE_PROJECT)
                ->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}

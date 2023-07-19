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
                    ->values([Config::MODE_PROJECT, Config::MODE_ORGANIZATION, Config::MODE_ACTIVITY_CENTER])
                    ->defaultValue(Config::MODE_PROJECT)
                ->end()
                ->booleanNode('incrementalFetching')->defaultFalse()->end()
                ->booleanNode('incremental')->defaultFalse()->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}

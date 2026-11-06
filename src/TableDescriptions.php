<?php

declare(strict_types=1);

namespace Keboola\TelemetryData;

/**
 * Table descriptions of the active telemetry tables, taken from
 * https://help.keboola.com/components/extractors/other/telemetry-data/
 */
class TableDescriptions
{
    private const DESCRIPTIONS = [
        'contract_limit_monthly' =>
            'This table lists the limits set in contracts for comparison with actual consumption.',
        'kbc_bigquery_stats' =>
            'This table shows information about queries using BigQuery, including transformations and ' .
            'sandboxes. Note that the table is not generated for BYODB customers.',
        'kbc_branch' =>
            'This table shows main and development [branches](https://help.keboola.com/components/branches/) ' .
            'in the project.',
        'kbc_bucket' =>
            'This table shows data about the current state of storage buckets. Note: The table is always ' .
            'extracted in full.',
        'kbc_column' =>
            'This table shows data about the current state of storage columns. Note: The table is always ' .
            'extracted in full.',
        'kbc_column_metadata' =>
            'This table shows data about columns\' metadata. Note: The table is always extracted in full.',
        'kbc_component_configuration' =>
            'This table lists the [configurations of ' .
            'components](https://help.keboola.com/components/#creating-component-configuration) (e.g., a ' .
            'configuration of the AWS S3 data source connector). Note: The table is always extracted in ' .
            'full.',
        'kbc_component_configuration_row' =>
            'This table lists rows of the [configurations for the ' .
            'components](https://help.keboola.com/components/#creating-component-configuration) (e.g., a ' .
            'configuration for a single table in the Snowflake data destination connector).',
        'kbc_component_configuration_version' =>
            'This table shows the [version ' .
            'history](https://help.keboola.com/components/#configuration-versions) of the component ' .
            'configuration.',
        'kbc_data_app_params_event' =>
            'This table lists events and their non-empty parameters from the ext.keboola.data-apps. event.',
        'kbc_file_event' =>
            'This table shows data about storage [file](https://help.keboola.com/storage/files/) events.',
        'kbc_flow_phase' =>
            'This table shows data about particular phases of flows. Note: The table is always extracted in ' .
            'full.',
        'kbc_flow_task' => 'This table shows data about flow tasks. Note: The table is always extracted in full.',
        'kbc_job' =>
            'This table lists Keboola [jobs](https://help.keboola.com/management/jobs/) (e.g., a connector ' .
            'job or a transformation).',
        'kbc_mcp_event' =>
            'This table captures [MCP](https://help.keboola.com/ai/mcp-server/) (Model Context Protocol) ' .
            'interaction events – every tool call made against a Keboola project through an MCP server. This ' .
            'includes calls from external MCP clients (e.g., Claude Desktop, Cursor) as well as Keboola\'s ' .
            'internal AI agents such as [Kai](https://help.keboola.com/kai/) and AI Chat. Each row ' .
            'represents a single tool invocation with its input arguments, outcome (success or error), ' .
            'duration, and the context of the caller.',
        'kbc_non_sql_workspace' =>
            'This table shows data about non-SQL [workspaces](https://help.keboola.com/workspace/), ' .
            'including Apps (Streamlit, Python/JS) and Data Science sandboxes (Python, R, etc.).',
        'kbc_non_sql_workspace_run' =>
            'This table lists runs of non-SQL [workspaces](https://help.keboola.com/workspace/) (Apps and ' .
            'Data Science sandboxes) and their consumption metrics. It replaces kbc_data_app_workspace and ' .
            'kbc_data_science_sandbox for non-SQL workspace run tracking.',
        'kbc_notification_subscription' =>
            'This table shows data subscriptions to notifications sent by Keboola (mostly flow ' .
            'notifications). Note: The table is always extracted in full.',
        'kbc_organization' =>
            'This table shows data about Keboola ' .
            '[organizations](https://help.keboola.com/management/organization/).',
        'kbc_organization_user' =>
            'This table shows data about users in ' .
            '[organizations](https://help.keboola.com/management/organization/).',
        'kbc_project' =>
            'This table shows data about Keboola [projects](https://help.keboola.com/management/project/) ' .
            'belonging to an organization.',
        'kbc_project_snapshot' => 'This table shows snapshots of projects to track changes in their size.',
        'kbc_project_user' =>
            'This table shows snapshots capturing the projects\' ' .
            '[admins](https://help.keboola.com/management/project/users/). The snapshots are taken multiple ' .
            'times a day.',
        'kbc_schedule' =>
            'This table shows data about configuration schedules. Note: The table is always extracted in ' .
            'full.',
        'kbc_snowflake_stats' =>
            'This table shows information about queries using Snowflake, including transformations and ' .
            'sandboxes. Note that the table is not generated for BYODB customers.',
        'kbc_sql_workspace' =>
            'This table shows data about SQL [workspaces](https://help.keboola.com/workspace/) provisioned ' .
            'via configurations. It replaces kbc_workspace for SQL-based workspace tracking.',
        'kbc_table' =>
            'This table shows data about the current state of storage ' .
            '[tables](https://help.keboola.com/storage/tables/) in the main branch only. Note: The table is ' .
            'always extracted in full.',
        'kbc_table_event' =>
            'This table shows data about events of the storage ' .
            '[tables](https://help.keboola.com/storage/tables/).',
        'kbc_table_metadata' =>
            'This table shows data about metadata of the storage ' .
            '[tables](https://help.keboola.com/storage/tables/). Note: The table is always extracted in ' .
            'full.',
        'kbc_token' =>
            'This table shows data about the storage ' .
            '[tokens](https://help.keboola.com/management/project/tokens/). Note: The table is always ' .
            'extracted in full.',
        'kbc_token_event' =>
            'This table shows data about events of the storage ' .
            '[tokens](https://help.keboola.com/management/project/tokens/).',
        'kbc_transformation_code' =>
            'This table shows data about codes of ' .
            '[transformations](https://help.keboola.com/transformations/). Note: The table is always ' .
            'extracted in full.',
        'kbc_trigger' =>
            'This table shows data about ' .
            '[triggers](https://help.keboola.com/components/applications/triggers/). Note: The table is ' .
            'always extracted in full.',
        'kbc_trigger_table' =>
            'This table shows data about tables used to ' .
            '[trigger](https://help.keboola.com/components/applications/triggers/) jobs. Note: The table is ' .
            'always extracted in full.',
        'kbc_usage_metrics_values' =>
            'This table shows aggregated values for all metrics that may be included in the contract, such ' .
            'as consumed credits, data in storage, and the number of users. This combines data from ' .
            'different data sources (tables), making it useful for providing an overview of consumption. ' .
            'Note: organization_value and company_value are available in Organization mode only. You need ' .
            'data for all projects.',
        'kbc_user_activity' =>
            'This table shows data about basic activity of project users (job runs and configuration ' .
            'updates).',
        'kbc_workspace_event' => 'This table shows data about [workspace](https://help.keboola.com/workspace/) events.',
        'security_event' =>
            'This table lists [security ' .
            'events](https://help.keboola.com/management/project/tokens/#token-events), such as project ' .
            'logins or token creations. The events might be related directly to the project, its ' .
            'organization, or the Keboola platform itself.',
        'usage_metric' => 'This table shows metrics related to your contracts.',
    ];

    public static function get(string $tableName): ?string
    {
        return self::DESCRIPTIONS[strtolower($tableName)] ?? null;
    }
}

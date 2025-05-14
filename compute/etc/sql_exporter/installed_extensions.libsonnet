{
    metric_name: 'installed_extensions',
    type: 'gauge',
    help: 'List of installed extensions',
    key_labels: ['extension_name'],
    values: [
        'extension_name',
        'installed_version',
    ],
    query: importstr 'sql_exporter/installed_extensions.sql',
}
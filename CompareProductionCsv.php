<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Artisan;
use Exception;

class CompareSchemaWithProduction extends Command
{
    protected $signature = 'schema:compare {csv : Path to production schema CSV} {--skip-migrate : Skip running migrations}';
    protected $description = 'Run fresh migrations on shadow DB and compare with production CSV';

    private $production = [];
    private $migration = [];

    public function handle()
    {
        $csvPath = $this->argument('csv');
        
        if (!file_exists($csvPath)) {
            $this->error("CSV file not found: {$csvPath}");
            return 1;
        }

        // Run fresh migrations on shadow database unless skipped
        if (!$this->option('skip-migrate')) {
            $this->info('Running fresh migrations on mysql_shadow connection...');
            $this->line('');
            
            try {
                // Run with output
                $exitCode = Artisan::call('migrate:fresh', [
                    '--database' => 'mysql_shadow',
                    '--force' => true,
                ], $this->output);
                
                if ($exitCode !== 0) {
                    $this->error('Migration failed! Check the migration order and dependencies.');
                    $this->line('');
                    $this->info('Tip: Views need their base tables to exist first. Check migration timestamps.');
                    $this->line('You can run with --skip-migrate to compare existing shadow database');
                    return 1;
                }
                
                $this->info('✓ Migrations completed successfully');
            } catch (Exception $e) {
                $this->error('Migration failed with error:');
                $this->error($e->getMessage());
                $this->line('');
                $this->info('Common issues:');
                $this->line('  • Views referencing tables that don\'t exist yet (wrong migration order)');
                $this->line('  • Foreign keys referencing tables not yet created');
                $this->line('  • Check your migration file timestamps and dependencies');
                $this->line('');
                $this->line('You can run with --skip-migrate to compare existing shadow database');
                return 1;
            }
        } else {
            $this->info('Skipping migrations, using existing shadow database...');
        }
        
        $this->line('');

        // Parse production CSV
        $this->parseProductionCsv($csvPath);
        
        // Get migration schema from shadow database
        $this->getMigrationSchema();
        
        // Compare and display differences
        $this->compareAndDisplay();

        return 0;
    }

    private function parseProductionCsv($csvPath)
    {
        $this->info('Parsing production CSV...');
        
        $handle = fopen($csvPath, 'r');
        $headers = fgetcsv($handle);
        
        // Clean headers - remove any BOM and trim whitespace
        $headers = array_map(function($h) {
            return strtolower(trim($h, " \t\n\r\0\x0B\xEF\xBB\xBF"));
        }, $headers);
        
        $rowCount = 0;
        $viewsList = [];
        
        while (($row = fgetcsv($handle)) !== false) {
            $rowCount++;
            if (count($row) !== count($headers)) continue;
            
            $data = array_combine($headers, $row);
            
            // Skip if no table name
            if (empty($data['table_name'])) continue;
            
            $table = $data['table_name'];
            
            // Track table types
            if (!empty($data['table_type'])) {
                if (strtoupper($data['table_type']) === 'VIEW') {
                    $viewsList[] = $table;
                    continue; // Skip views for now
                }
            }
            
            // Skip if no column name (might be a table-only row)
            if (empty($data['column_name'])) continue;
            
            $column = $data['column_name'];
            
            // Initialize table if needed
            if (!isset($this->production[$table])) {
                $this->production[$table] = [
                    'columns' => [],
                    'indexes' => []
                ];
            }
            
            // Store column data
            $this->production[$table]['columns'][$column] = [
                'type' => $this->normalizeType($data['column_type'] ?? $data['data_type'] ?? ''),
                'nullable' => strtoupper($data['is_nullable'] ?? 'NO') === 'YES',
                'default' => $this->normalizeDefault($data['column_default'] ?? null),
                'key' => $data['column_key'] ?? '',
                'extra' => strtolower($data['extra'] ?? ''),
            ];
            
            // Store index data if present
            if (!empty($data['index_name'])) {
                $indexName = $data['index_name'];
                if (!isset($this->production[$table]['indexes'][$indexName])) {
                    $this->production[$table]['indexes'][$indexName] = [
                        'unique' => ($data['non_unique'] ?? '1') === '0',
                        'columns' => []
                    ];
                }
                if (!in_array($column, $this->production[$table]['indexes'][$indexName]['columns'])) {
                    $this->production[$table]['indexes'][$indexName]['columns'][] = $column;
                }
            }
        }
        
        fclose($handle);
        
        $this->info('✓ Parsed ' . count($this->production) . ' tables from production CSV');
        if (!empty($viewsList)) {
            $this->info('  (Skipped ' . count($viewsList) . ' views: ' . implode(', ', array_slice($viewsList, 0, 5)) . 
                       (count($viewsList) > 5 ? '...' : '') . ')');
        }
        $this->line("  Total rows processed: {$rowCount}");
    }

    private function getMigrationSchema()
    {
        $this->info('Reading migration schema from shadow database...');
        
        try {
            $tables = DB::connection('mysql_shadow')->select('SHOW FULL TABLES');
            $dbName = DB::connection('mysql_shadow')->getDatabaseName();
            
            $tableCount = 0;
            $viewCount = 0;
            
            foreach ($tables as $tableObj) {
                // Get table name and type
                $tableKey = "Tables_in_{$dbName}";
                $typeKey = "Table_type";
                
                $tableName = $tableObj->$tableKey;
                $tableType = $tableObj->$typeKey ?? 'BASE TABLE';
                
                // Skip views and migrations table
                if ($tableType === 'VIEW' || $tableName === 'migrations') {
                    if ($tableType === 'VIEW') $viewCount++;
                    continue;
                }
                
                $tableCount++;
                
                // Initialize table structure
                $this->migration[$tableName] = [
                    'columns' => [],
                    'indexes' => []
                ];
                
                // Get columns
                $columns = DB::connection('mysql_shadow')->select("SHOW FULL COLUMNS FROM `{$tableName}`");
                foreach ($columns as $col) {
                    $this->migration[$tableName]['columns'][$col->Field] = [
                        'type' => $this->normalizeType($col->Type),
                        'nullable' => $col->Null === 'YES',
                        'default' => $this->normalizeDefault($col->Default),
                        'key' => $col->Key,
                        'extra' => strtolower($col->Extra),
                    ];
                }
                
                // Get indexes
                $indexes = DB::connection('mysql_shadow')->select("SHOW INDEXES FROM `{$tableName}`");
                foreach ($indexes as $idx) {
                    if (!isset($this->migration[$tableName]['indexes'][$idx->Key_name])) {
                        $this->migration[$tableName]['indexes'][$idx->Key_name] = [
                            'unique' => !$idx->Non_unique,
                            'columns' => []
                        ];
                    }
                    $this->migration[$tableName]['indexes'][$idx->Key_name]['columns'][] = $idx->Column_name;
                }
            }
            
            $this->info('✓ Read ' . $tableCount . ' tables from migration database');
            if ($viewCount > 0) {
                $this->info("  (Skipped {$viewCount} views)");
            }
            
        } catch (Exception $e) {
            $this->error('Failed to read migration schema: ' . $e->getMessage());
            throw $e;
        }
    }

    private function normalizeType($type)
    {
        $type = strtolower(trim($type));
        
        // Remove display widths from integers
        $type = preg_replace('/^(tinyint|smallint|mediumint|int|bigint)\(\d+\)/', '$1', $type);
        
        // Normalize unsigned
        $type = preg_replace('/\s+unsigned/', ' unsigned', $type);
        
        // Remove character set and collation info
        $type = preg_replace('/\s+(character set|charset|collate)\s+\S+/i', '', $type);
        
        // Normalize spaces
        $type = preg_replace('/\s+/', ' ', $type);
        
        return trim($type);
    }

    private function normalizeDefault($default)
    {
        if ($default === null || strtoupper($default) === 'NULL') {
            return null;
        }
        
        // Handle CURRENT_TIMESTAMP variations
        if (stripos($default, 'CURRENT_TIMESTAMP') !== false || 
            stripos($default, 'current_timestamp()') !== false) {
            return 'CURRENT_TIMESTAMP';
        }
        
        // Remove surrounding quotes
        $default = trim($default, "'\"");
        
        // Empty string
        if ($default === '') {
            return "''";
        }
        
        return $default;
    }

    private function compareAndDisplay()
    {
        $this->line('');
        $this->info('══════════════════════════════════════════════════════════════');
        $this->info('                    SCHEMA COMPARISON RESULTS                    ');
        $this->info('══════════════════════════════════════════════════════════════');
        $this->line('');

        $hasIssues = false;
        $stats = [
            'missing_tables' => 0,
            'extra_tables' => 0,
            'missing_columns' => 0,
            'extra_columns' => 0,
            'column_diffs' => 0,
            'missing_indexes' => 0,
        ];

        // Check for missing tables
        $missingTables = array_diff(array_keys($this->production), array_keys($this->migration));
        if (!empty($missingTables)) {
            $hasIssues = true;
            $stats['missing_tables'] = count($missingTables);
            $this->error('▸ MISSING TABLES (exist in production, not in migrations):');
            foreach ($missingTables as $table) {
                $this->line("    • {$table}");
                $columnCount = count($this->production[$table]['columns'] ?? []);
                $this->line("      ({$columnCount} columns in production)");
            }
            $this->line('');
        }

        // Check for extra tables
        $extraTables = array_diff(array_keys($this->migration), array_keys($this->production));
        if (!empty($extraTables)) {
            $hasIssues = true;
            $stats['extra_tables'] = count($extraTables);
            $this->warn('▸ EXTRA TABLES (exist in migrations, not in production):');
            foreach ($extraTables as $table) {
                $this->line("    • {$table}");
            }
            $this->line('');
        }

        // Compare table structures
        foreach ($this->production as $table => $prodData) {
            if (!isset($this->migration[$table])) continue;
            
            $migData = $this->migration[$table];
            $tableIssues = [];
            
            // Check columns
            $prodCols = $prodData['columns'] ?? [];
            $migCols = $migData['columns'] ?? [];
            
            // Missing columns
            $missingCols = array_diff(array_keys($prodCols), array_keys($migCols));
            if (!empty($missingCols)) {
                $stats['missing_columns'] += count($missingCols);
                foreach ($missingCols as $col) {
                    $tableIssues[] = [
                        'type' => 'missing_column',
                        'column' => $col,
                        'details' => $prodCols[$col]
                    ];
                }
            }
            
            // Extra columns
            $extraCols = array_diff(array_keys($migCols), array_keys($prodCols));
            if (!empty($extraCols)) {
                $stats['extra_columns'] += count($extraCols);
                foreach ($extraCols as $col) {
                    $tableIssues[] = [
                        'type' => 'extra_column',
                        'column' => $col
                    ];
                }
            }
            
            // Column differences
            foreach ($prodCols as $col => $prodCol) {
                if (!isset($migCols[$col])) continue;
                
                $migCol = $migCols[$col];
                $colDiffs = [];
                
                if ($prodCol['type'] !== $migCol['type']) {
                    $colDiffs[] = "type: '{$prodCol['type']}' → '{$migCol['type']}'";
                }
                
                if ($prodCol['nullable'] !== $migCol['nullable']) {
                    $prodNull = $prodCol['nullable'] ? 'NULL' : 'NOT NULL';
                    $migNull = $migCol['nullable'] ? 'NULL' : 'NOT NULL';
                    $colDiffs[] = "nullable: {$prodNull} → {$migNull}";
                }
                
                if ($prodCol['default'] !== $migCol['default']) {
                    $prodDef = $prodCol['default'] ?? 'NULL';
                    $migDef = $migCol['default'] ?? 'NULL';
                    $colDiffs[] = "default: {$prodDef} → {$migDef}";
                }
                
                if ($prodCol['extra'] !== $migCol['extra'] && !empty($prodCol['extra'])) {
                    $colDiffs[] = "extra: '{$prodCol['extra']}' → '{$migCol['extra']}'";
                }
                
                if (!empty($colDiffs)) {
                    $stats['column_diffs']++;
                    $tableIssues[] = [
                        'type' => 'column_diff',
                        'column' => $col,
                        'differences' => $colDiffs
                    ];
                }
            }
            
            // Check indexes
            $prodIndexes = $prodData['indexes'] ?? [];
            $migIndexes = $migData['indexes'] ?? [];
            
            // Missing indexes
            foreach ($prodIndexes as $idxName => $prodIdx) {
                if (!isset($migIndexes[$idxName])) {
                    $stats['missing_indexes']++;
                    $type = $prodIdx['unique'] ? 'UNIQUE' : 'INDEX';
                    $cols = implode(', ', $prodIdx['columns']);
                    $tableIssues[] = [
                        'type' => 'missing_index',
                        'index' => $idxName,
                        'details' => "{$type} ({$cols})"
                    ];
                }
            }
            
            // Display table issues
            if (!empty($tableIssues)) {
                $hasIssues = true;
                $this->error("▸ TABLE: {$table}");
                
                foreach ($tableIssues as $issue) {
                    switch ($issue['type']) {
                        case 'missing_column':
                            $this->line("    ❌ Missing column: {$issue['column']}");
                            $this->line("       Type: {$issue['details']['type']}");
                            break;
                            
                        case 'extra_column':
                            $this->line("    ⚠️  Extra column: {$issue['column']}");
                            break;
                            
                        case 'column_diff':
                            $this->line("    ⚠️  Column differs: {$issue['column']}");
                            foreach ($issue['differences'] as $diff) {
                                $this->line("       • {$diff}");
                            }
                            break;
                            
                        case 'missing_index':
                            $this->line("    ❌ Missing index: {$issue['index']} {$issue['details']}");
                            break;
                    }
                }
                $this->line('');
            }
        }

        // Summary
        $this->line('──────────────────────────────────────────────────────────────');
        if (!$hasIssues) {
            $this->info('✅ SUCCESS: Migrations match production schema perfectly!');
        } else {
            $this->error('📊 SUMMARY OF DIFFERENCES:');
            $this->line('');
            if ($stats['missing_tables'] > 0) {
                $this->line("   Missing tables:  {$stats['missing_tables']}");
            }
            if ($stats['extra_tables'] > 0) {
                $this->line("   Extra tables:    {$stats['extra_tables']}");
            }
            if ($stats['missing_columns'] > 0) {
                $this->line("   Missing columns: {$stats['missing_columns']}");
            }
            if ($stats['extra_columns'] > 0) {
                $this->line("   Extra columns:   {$stats['extra_columns']}");
            }
            if ($stats['column_diffs'] > 0) {
                $this->line("   Column diffs:    {$stats['column_diffs']}");
            }
            if ($stats['missing_indexes'] > 0) {
                $this->line("   Missing indexes: {$stats['missing_indexes']}");
            }
            $this->line('');
            $this->warn('⚠️  Fix the issues above to match production schema');
        }
        $this->line('');
    }
}

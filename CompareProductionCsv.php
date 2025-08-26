<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Artisan;

class CompareSchemaWithProduction extends Command
{
    protected $signature = 'schema:compare {csv : Path to production schema CSV}';
    protected $description = 'Run fresh migrations on shadow DB and compare with production CSV';

    private $production = [];
    private $migration = [];
    private $differences = [];

    public function handle()
    {
        $csvPath = $this->argument('csv');
        
        if (!file_exists($csvPath)) {
            $this->error("CSV file not found: {$csvPath}");
            return 1;
        }

        // Run fresh migrations on shadow database
        $this->info('Running fresh migrations on mysql_shadow connection...');
        Artisan::call('migrate:fresh', [
            '--database' => 'mysql_shadow',
            '--force' => true,
        ]);
        $this->info('✓ Migrations completed');
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
        
        while (($row = fgetcsv($handle)) !== false) {
            if (count($row) !== count($headers)) continue;
            
            $data = array_combine($headers, $row);
            if (empty($data['table_name']) || empty($data['column_name'])) continue;
            
            $table = $data['table_name'];
            $column = $data['column_name'];
            
            // Store column data
            $this->production[$table]['columns'][$column] = [
                'type' => $this->normalizeType($data['column_type'] ?? ''),
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
                $this->production[$table]['indexes'][$indexName]['columns'][] = $column;
            }
        }
        
        fclose($handle);
        $this->info('✓ Parsed ' . count($this->production) . ' tables from production CSV');
    }

    private function getMigrationSchema()
    {
        $this->info('Reading migration schema from shadow database...');
        
        $tables = DB::connection('mysql_shadow')->select('SHOW TABLES');
        $dbName = DB::connection('mysql_shadow')->getDatabaseName();
        $tableKey = "Tables_in_{$dbName}";
        
        foreach ($tables as $tableObj) {
            $table = $tableObj->$tableKey;
            
            // Skip migrations table
            if ($table === 'migrations') continue;
            
            // Get columns
            $columns = DB::connection('mysql_shadow')->select("SHOW FULL COLUMNS FROM `{$table}`");
            foreach ($columns as $col) {
                $this->migration[$table]['columns'][$col->Field] = [
                    'type' => $this->normalizeType($col->Type),
                    'nullable' => $col->Null === 'YES',
                    'default' => $this->normalizeDefault($col->Default),
                    'key' => $col->Key,
                    'extra' => strtolower($col->Extra),
                ];
            }
            
            // Get indexes
            $indexes = DB::connection('mysql_shadow')->select("SHOW INDEXES FROM `{$table}`");
            foreach ($indexes as $idx) {
                if (!isset($this->migration[$table]['indexes'][$idx->Key_name])) {
                    $this->migration[$table]['indexes'][$idx->Key_name] = [
                        'unique' => !$idx->Non_unique,
                        'columns' => []
                    ];
                }
                $this->migration[$table]['indexes'][$idx->Key_name]['columns'][] = $idx->Column_name;
            }
        }
        
        $this->info('✓ Read ' . count($this->migration) . ' tables from migration database');
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

        // Check for missing tables
        $missingTables = array_diff(array_keys($this->production), array_keys($this->migration));
        if (!empty($missingTables)) {
            $hasIssues = true;
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
        if (!$hasIssues) {
            $this->line('');
            $this->info('✅ SUCCESS: Migrations match production schema perfectly!');
            $this->line('');
        } else {
            $this->line('──────────────────────────────────────────────────────────────');
            $this->error('⚠️  Fix the issues above to match production schema');
            $this->line('');
            $this->line('Legend:');
            $this->line('  ❌ Critical: Missing in migrations (exists in production)');
            $this->line('  ⚠️  Warning: Extra in migrations or differs from production');
            $this->line('');
        }
    }
}

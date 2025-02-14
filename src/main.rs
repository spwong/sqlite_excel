
use calamine::{open_workbook, Reader, Xlsx};
use rusqlite::{params_from_iter, Connection, Result};
//use std::time::Instant;


// use rusqlite::{Connection, Result};
use xlsxwriter::*;
use std::error::Error;
use std::env;


fn export_to_excel(db_path: &str, excel_path: &str) -> Result<(), Box<dyn Error>> {
    // Connect to the SQLite database
    let conn = Connection::open(db_path)?;

    // Get all table names
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
    let tables: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    // Create a new XLSX file
    let workbook = Workbook::new(excel_path)?;

    // Process each table
    for table_name in tables {
        // Create a worksheet for each table
        let mut sheet = workbook.add_worksheet(Some(&table_name))?;

        // Get the column names and types
        let mut stmt = conn.prepare(&format!("PRAGMA table_info([{}])", table_name))?;
        let columns: Vec<(String, String)> = stmt
            .query_map([], |row| Ok((row.get(1)?, row.get(2)?)))?
            .collect::<Result<Vec<_>, _>>()?;

        // Write the column names to the first row
        for (col_index, (col_name, _)) in columns.iter().enumerate() {
            sheet.write_string(0, col_index as u16, col_name, None)?;
        }

        // Get the table data
        let mut stmt = conn.prepare(&format!("SELECT * FROM [{}]", table_name))?;
        let mut rows = stmt.query([])?;

        // Write the table data to the worksheet
        let mut row_index = 1;
        while let Some(row) = rows.next()? {
            for (col_index, _) in columns.iter().enumerate() {
                let value: Option<String> = row.get(col_index)?;
                let value = value.unwrap_or_else(|| "".to_string());
                sheet.write_string(row_index, col_index as u16, &value, None)?;
            }
            row_index += 1;
        }
    }

    //println!("Export complete: {}", excel_path);
    Ok(())
}

fn import_from_excel(excel_path: &str, db_path: &str) -> Result<(), Box<dyn Error>> {
    // ...existing code...
    let mut workbook: Xlsx<_> = open_workbook(excel_path)?;
    let conn = Connection::open(db_path)?;
    let txn = conn.unchecked_transaction()?;

    for sheet_name in workbook.sheet_names().to_owned() {
        if let Ok(range) = workbook.worksheet_range(&sheet_name) {
            let mut rows = range.rows();
            if let Some(header_row) = rows.next() {
                let mut col_names: Vec<String> = header_row.iter().map(|cell| cell.to_string()).collect();

                // changed code: rename duplicated columns
                use std::collections::HashSet;
                let mut seen = HashSet::new();
                let mut counter = 1;
                for name in col_names.iter_mut() {
                    if !seen.insert(name.clone()) {
                        *name = format!("col{}", counter);
                        seen.insert(name.clone());
                        counter += 1;
                    }
                }

                // ...existing code...
                let col_defs = col_names
                    .iter()
                    .map(|name| format!("[{}] TEXT", name))
                    .collect::<Vec<_>>()
                    .join(", ");
                let col_defs = format!("{}, [row_number] INTEGER", col_defs);

                let create_sql = format!("CREATE TABLE IF NOT EXISTS [{}] ({})", sheet_name, col_defs);
                conn.execute(&create_sql, [])?;

                let escaped_cols = col_names
                    .iter()
                    .map(|name| format!("[{}]", name))
                    .collect::<Vec<_>>()
                    .join(", ");
                let escaped_cols_with_row = format!("{}, [row_number]", escaped_cols);

                let placeholders = (0..col_names.len() + 1)
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(", ");
                let insert_sql = format!(
                    "INSERT INTO [{}] ({}) VALUES ({})",
                    sheet_name, escaped_cols_with_row, placeholders
                );

                let mut row_count = 1;
                for row in rows {
                    let row_values: Vec<String> = row.iter().map(|cell| cell.to_string()).collect();
                    let mut param_values = row_values.clone();
                    param_values.push(row_count.to_string());
                    let param_iter = param_values.iter().map(|s| s.as_str());
                    txn.execute(&insert_sql, params_from_iter(param_iter))?;
                    row_count += 1;
                }
            }
        }
    }

    txn.commit()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    // Get command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Sqlite and Excel Export/Import tool v1.0 by CSA(1)3 written in Rust");
        eprintln!("Usage: {} --export <database_path> <output_excel>", args[0]);
        eprintln!("Usage: {} --import <excel_path> <output_database>", args[0]);
        std::process::exit(1);
    }
    let command = &args[1];
    let input_path = &args[2];
    let output_path = &args[3];

    match command.as_str() {
        "--export" => export_to_excel(input_path, output_path),
        "--import" => import_from_excel(input_path, output_path),
        _ => {
            eprintln!("Sqlite and Excel Export/Import tool v1.0 by CSA(1)3 written in Rust");
            eprintln!("Invalid command: {}", command);
            eprintln!("Usage: {} --export <database_path> <output_excel>", args[0]);
            eprintln!("Usage: {} --import <excel_path> <output_database>", args[0]);
            std::process::exit(1);
        }
    }
}
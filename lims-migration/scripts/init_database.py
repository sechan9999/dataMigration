#!/usr/bin/env python3
"""
LIMS Migration - Database Initialization Script
Initializes SQL database schema and stored procedures
"""

import json
import os
import sys
from pathlib import Path
import pyodbc


class DatabaseInitializer:
    """Handles database schema initialization"""

    def __init__(self, config_path: str):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        self.server = f"{self.config['sql_database']['server_name']}.database.windows.net"
        self.database = self.config['sql_database']['database_name']
        self.sql_path = Path(__file__).parent.parent / 'sql'

    def get_connection(self):
        """Create database connection"""
        # Using Azure AD authentication
        connection_string = f"""
        Driver={{ODBC Driver 17 for SQL Server}};
        Server={self.server};
        Database={self.database};
        Authentication=ActiveDirectoryInteractive;
        Encrypt=yes;
        TrustServerCertificate=no;
        Connection Timeout=30;
        """

        return pyodbc.connect(connection_string)

    def execute_sql_file(self, conn, file_path: Path):
        """Execute SQL from file"""
        print(f"Executing: {file_path.name}")

        with open(file_path, 'r') as f:
            sql = f.read()

        # Split by GO statement
        batches = [b.strip() for b in sql.split('GO') if b.strip()]

        cursor = conn.cursor()
        try:
            for batch in batches:
                if batch:
                    cursor.execute(batch)
                    conn.commit()
            print(f"  ✓ Success")
            return True
        except Exception as e:
            print(f"  ❌ Error: {str(e)}")
            conn.rollback()
            return False

    def create_schemas(self, conn):
        """Create database schemas"""
        print("\n" + "="*80)
        print("Creating Schemas")
        print("="*80)

        schema_file = self.sql_path / 'ddl' / '01_create_schemas.sql'
        return self.execute_sql_file(conn, schema_file)

    def create_dimension_tables(self, conn):
        """Create dimension tables"""
        print("\n" + "="*80)
        print("Creating Dimension Tables")
        print("="*80)

        dim_file = self.sql_path / 'ddl' / '02_create_dimension_tables.sql'
        return self.execute_sql_file(conn, dim_file)

    def create_fact_tables(self, conn):
        """Create fact tables"""
        print("\n" + "="*80)
        print("Creating Fact Tables")
        print("="*80)

        fact_file = self.sql_path / 'ddl' / '03_create_fact_tables.sql'
        return self.execute_sql_file(conn, fact_file)

    def create_etl_tables(self, conn):
        """Create ETL control tables"""
        print("\n" + "="*80)
        print("Creating ETL Tables")
        print("="*80)

        etl_file = self.sql_path / 'ddl' / '04_create_etl_tables.sql'
        return self.execute_sql_file(conn, etl_file)

    def create_stored_procedures(self, conn):
        """Create stored procedures"""
        print("\n" + "="*80)
        print("Creating Stored Procedures")
        print("="*80)

        sp_dir = self.sql_path / 'stored_procedures'

        for sp_file in sorted(sp_dir.glob('*.sql')):
            self.execute_sql_file(conn, sp_file)

        return True

    def create_views(self, conn):
        """Create views"""
        print("\n" + "="*80)
        print("Creating Views")
        print("="*80)

        view_dir = self.sql_path / 'views'

        for view_file in sorted(view_dir.glob('*.sql')):
            self.execute_sql_file(conn, view_file)

        return True

    def populate_date_dimension(self, conn):
        """Populate date dimension table"""
        print("\n" + "="*80)
        print("Populating Date Dimension")
        print("="*80)

        # Generate dates for 10 years (5 years back, 5 years forward)
        sql = """
        DECLARE @StartDate DATE = DATEADD(YEAR, -5, GETDATE());
        DECLARE @EndDate DATE = DATEADD(YEAR, 5, GETDATE());

        WITH DateSequence AS (
            SELECT @StartDate AS Date
            UNION ALL
            SELECT DATEADD(DAY, 1, Date)
            FROM DateSequence
            WHERE Date < @EndDate
        )
        INSERT INTO dim.DimDate (
            DateKey, Date, Year, Quarter, Month, MonthName,
            Week, DayOfYear, DayOfMonth, DayOfWeek, DayName,
            IsWeekend, IsHoliday, FiscalYear, FiscalQuarter, FiscalMonth
        )
        SELECT
            CAST(FORMAT(Date, 'yyyyMMdd') AS INT) AS DateKey,
            Date,
            YEAR(Date) AS Year,
            DATEPART(QUARTER, Date) AS Quarter,
            MONTH(Date) AS Month,
            DATENAME(MONTH, Date) AS MonthName,
            DATEPART(WEEK, Date) AS Week,
            DATEPART(DAYOFYEAR, Date) AS DayOfYear,
            DAY(Date) AS DayOfMonth,
            DATEPART(WEEKDAY, Date) AS DayOfWeek,
            DATENAME(WEEKDAY, Date) AS DayName,
            CASE WHEN DATEPART(WEEKDAY, Date) IN (1, 7) THEN 1 ELSE 0 END AS IsWeekend,
            0 AS IsHoliday,
            CASE WHEN MONTH(Date) >= 7 THEN YEAR(Date) + 1 ELSE YEAR(Date) END AS FiscalYear,
            CASE
                WHEN MONTH(Date) BETWEEN 7 AND 9 THEN 1
                WHEN MONTH(Date) BETWEEN 10 AND 12 THEN 2
                WHEN MONTH(Date) BETWEEN 1 AND 3 THEN 3
                ELSE 4
            END AS FiscalQuarter,
            CASE
                WHEN MONTH(Date) >= 7 THEN MONTH(Date) - 6
                ELSE MONTH(Date) + 6
            END AS FiscalMonth
        FROM DateSequence
        WHERE NOT EXISTS (
            SELECT 1 FROM dim.DimDate d WHERE d.Date = DateSequence.Date
        )
        OPTION (MAXRECURSION 0);
        """

        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            conn.commit()
            row_count = cursor.rowcount
            print(f"✓ Populated {row_count:,} dates")
            return True
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            conn.rollback()
            return False

    def initialize_all(self):
        """Initialize all database components"""
        try:
            print("\n" + "="*80)
            print("LIMS Migration - Database Initialization")
            print("="*80)

            conn = self.get_connection()
            print("✓ Database connection established")

            self.create_schemas(conn)
            self.create_dimension_tables(conn)
            self.create_fact_tables(conn)
            self.create_etl_tables(conn)
            self.create_stored_procedures(conn)
            self.create_views(conn)
            self.populate_date_dimension(conn)

            conn.close()

            print("\n" + "="*80)
            print("✓ Database initialization completed successfully!")
            print("="*80)

            return True

        except Exception as e:
            print(f"\n❌ Initialization failed: {str(e)}")
            return False


def main():
    """Main execution"""
    config_path = Path(__file__).parent.parent / 'config' / 'azure_config.json'

    if not config_path.exists():
        print(f"❌ Configuration file not found: {config_path}")
        sys.exit(1)

    initializer = DatabaseInitializer(str(config_path))
    success = initializer.initialize_all()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

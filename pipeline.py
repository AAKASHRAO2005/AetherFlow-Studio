"""
Main ETL Pipeline Orchestrator.
Coordinates:
1. Extract (API, CSV, or Both)
2. Transform (Clean, Impute, Deduplicate, Enrich, Validate)
3. Load (Database initialization, Idempotent Upsert, Run Audit Logging)

Can be run directly via CLI or imported into automated schedulers/web interfaces.
"""

import argparse
from datetime import datetime, timezone
import logging
import sys
import time
from typing import Optional, Dict, Any

# Ensure UTF-8 output on Windows consoles to prevent cp1252 encoding errors
if sys.platform == "win32":
    try:
        if sys.stdout and hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        if sys.stderr and hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

from config import DATABASE_URL, RAW_DATA_DIR, TARGET_CITIES, PROJECT_NAME
from extractors.api_extractor import ApiExtractor
from extractors.csv_extractor import CsvExtractor
from transformers.cleaner import DataCleaner
from transformers.enricher import DataEnricher
from loaders.db_loader import DatabaseLoader

# Initialize Rich Console and Standard Logger
console = Console(legacy_windows=False)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(PROJECT_NAME)


class EtlPipeline:
    """
    Production-grade ETL pipeline teaching core data engineering concepts.
    """

    def __init__(self, db_url: str = DATABASE_URL):
        self.db_url = db_url
        self.api_extractor = ApiExtractor()
        self.csv_extractor = CsvExtractor()
        self.cleaner = DataCleaner()
        self.enricher = DataEnricher()
        self.loader = DatabaseLoader(db_url=self.db_url)

    def run_pipeline(
        self,
        source: str = "both",
        csv_file_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Executes the entire Extract -> Transform -> Load lifecycle.
        """
        start_time = time.time()
        started_at = datetime.now(timezone.utc).replace(tzinfo=None)
        total_extracted = 0
        total_transformed = 0
        total_loaded = 0
        status = "FAILED"
        error_msg = None

        console.print(Panel.fit(
            f"[bold cyan]🌊 {PROJECT_NAME} ETL Pipeline Execution[/bold cyan]\n"
            f"[dim]Source:[/dim] [yellow]{source.upper()}[/yellow] | "
            f"[dim]Database:[/dim] [green]{self.db_url.split('@')[-1]}[/green]",
            border_style="cyan"
        ))

        try:
            # -----------------------------------------------------------------
            # 1. EXTRACT STAGE
            # -----------------------------------------------------------------
            extracted_dfs = []

            if source in ["api", "both"]:
                with console.status("[bold blue]Extracting from Open-Meteo REST API...[/bold blue]"):
                    api_df = self.api_extractor.extract(cities=TARGET_CITIES)
                    if not api_df.empty:
                        api_df = self.cleaner.standardize_columns(api_df)
                        api_df["source_type"] = "API"
                        extracted_dfs.append(api_df)
                        console.print(f" [green][OK][/green] Extracted [bold]{len(api_df)}[/bold] records from Public Weather API")

            if source in ["csv", "both"]:
                with console.status("[bold blue]Extracting from CSV dataset...[/bold blue]"):
                    csv_df = self.csv_extractor.extract(file_path=csv_file_path)
                    if not csv_df.empty:
                        csv_df = self.cleaner.standardize_columns(csv_df)
                        csv_df["source_type"] = "CSV"
                        extracted_dfs.append(csv_df)
                        console.print(f" [green][OK][/green] Extracted [bold]{len(csv_df)}[/bold] records from CSV dataset")

            if not extracted_dfs:
                raise ValueError("No data extracted from any specified source.")

            raw_combined = pd.concat(extracted_dfs, ignore_index=True)
            total_extracted = len(raw_combined)

            # -----------------------------------------------------------------
            # 2. TRANSFORM STAGE
            # -----------------------------------------------------------------
            with console.status("[bold magenta]Transforming: Cleaning, Imputing & Deduplicating...[/bold magenta]"):
                cleaned_df, clean_stats = self.cleaner.clean(
                    raw_combined,
                    source_type=source.upper()
                )

            with console.status("[bold magenta]Transforming: Enriching with derived features...[/bold magenta]"):
                transformed_df = self.enricher.enrich(cleaned_df)
                total_transformed = len(transformed_df)

            # Data Quality Gate Validation
            quality_report = self.enricher.validate_quality(transformed_df)
            if not quality_report["passed"]:
                console.print(f"[bold red]Quality Gate Warning:[/bold red] Some checks flagged issues: {quality_report['checks']}")
            else:
                console.print(" [green][OK][/green] Data Quality Gate passed all validation rules")

            # Display Transformation Stats Table
            stats_table = Table(title="Transformation Audit Summary", box=box.ROUNDED)
            stats_table.add_column("Metric", style="cyan")
            stats_table.add_column("Count", justify="right", style="green")

            stats_table.add_row("Raw Extracted Rows", str(clean_stats["initial_rows"]))
            stats_table.add_row("Dropped Corrupted Dates/PKs", str(clean_stats["dropped_invalid_dates"]))
            stats_table.add_row("Outliers Filtered/Fixed", str(clean_stats["outliers_cleaned"]))
            stats_table.add_row("Missing Values Imputed", str(clean_stats["nulls_imputed"]))
            stats_table.add_row("Duplicates Removed", str(clean_stats["duplicates_removed"]))
            stats_table.add_row("Clean Transformed Rows Ready to Load", str(total_transformed))
            console.print(stats_table)

            # -----------------------------------------------------------------
            # 3. LOAD STAGE
            # -----------------------------------------------------------------
            with console.status("[bold yellow]Loading into Database with Idempotent Upsert...[/bold yellow]"):
                total_loaded = self.loader.load_weather_data(transformed_df)
                console.print(f" [green][OK][/green] Successfully upserted [bold]{total_loaded}[/bold] records into database")

            status = "SUCCESS"

        except Exception as e:
            status = "FAILED"
            error_msg = str(e)
            console.print(f"[bold red]Pipeline Execution Failed:[/bold red] {error_msg}")
            logger.exception("ETL execution error")

        finally:
            duration = time.time() - start_time
            # Record execution in pipeline audit log
            run_id = self.loader.record_pipeline_run(
                pipeline_name=PROJECT_NAME,
                source_name=source.upper(),
                status=status,
                rows_extracted=total_extracted,
                rows_transformed=total_transformed,
                rows_loaded=total_loaded,
                duration_seconds=duration,
                error_message=error_msg,
                started_at=started_at
            )

            # Display Final Summary Panel
            status_color = "green" if status == "SUCCESS" else "red"
            console.print(Panel(
                f"[bold {status_color}]Pipeline Run #{run_id}: {status}[/bold {status_color}]\n"
                f"- Extracted: [cyan]{total_extracted}[/cyan] rows\n"
                f"- Transformed: [magenta]{total_transformed}[/magenta] rows\n"
                f"- Loaded/Upserted: [green]{total_loaded}[/green] rows\n"
                f"- Duration: [yellow]{duration:.2f}s[/yellow]",
                title="Execution Results",
                border_style=status_color
            ))

        return {
            "run_id": run_id,
            "status": status,
            "extracted": total_extracted,
            "transformed": total_transformed,
            "loaded": total_loaded,
            "duration": duration,
            "error": error_msg
        }

    def print_db_summary(self) -> None:
        """
        Prints high-level stats and recent database observations.
        """
        metrics = self.loader.get_summary_metrics()
        table = Table(title="Database Inventory & Health", box=box.ROUNDED)
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Total Weather Records", str(metrics["total_observations"]))
        table.add_row("Distinct Cities Monitored", str(metrics["distinct_cities"]))
        table.add_row("Total ETL Runs Executed", str(metrics["total_runs"]))
        table.add_row("Successful ETL Runs", str(metrics["successful_runs"]))
        console.print(table)

        recent = self.loader.fetch_recent_observations(limit=5)
        if not recent.empty:
            rec_table = Table(title="Latest 5 Weather Observations in Database", box=box.SIMPLE_HEAVY)
            for col in ["city", "observation_time", "temperature_c", "humidity_percent", "weather_condition", "comfort_index", "source_type"]:
                rec_table.add_column(col)
            for _, row in recent.iterrows():
                rec_table.add_row(
                    str(row["city"]),
                    str(row["observation_time"]),
                    f"{row['temperature_c']}°C",
                    f"{row['humidity_percent']}%",
                    str(row["weather_condition"]),
                    str(row["comfort_index"]),
                    str(row["source_type"])
                )
            console.print(rec_table)


def main():
    parser = argparse.ArgumentParser(description="CSV/API to Database ETL Pipeline")
    parser.add_argument(
        "--source",
        choices=["api", "csv", "both"],
        default="both",
        help="Data extraction source (api, csv, or both)"
    )
    parser.add_argument(
        "--csv-file",
        type=str,
        default=None,
        help="Path to custom CSV file for extraction"
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default=DATABASE_URL,
        help="Database URL (SQLite or PostgreSQL)"
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print database summary and recent rows"
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Run custom SQL query on database"
    )

    args = parser.parse_args()

    pipeline = EtlPipeline(db_url=args.db_url)

    if args.query:
        console.print(f"[bold cyan]Executing SQL:[/bold cyan] {args.query}")
        result_df = pipeline.loader.execute_custom_sql(args.query)
        console.print(result_df)
        return

    if args.summary:
        pipeline.print_db_summary()
        return

    # Execute full ETL flow
    pipeline.run_pipeline(source=args.source, csv_file_path=args.csv_file)
    pipeline.print_db_summary()


if __name__ == "__main__":
    main()

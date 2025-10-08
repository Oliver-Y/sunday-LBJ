#!/usr/bin/env python3
"""
Silver layer: Process bronze parquet files for ML model preparation
"""
import polars as pl
import glob
import os
from pathlib import Path
import logging
import argparse

def get_logger(level_num:int): 
    level = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}.get(
        level_num, logging.DEBUG
    )
    logging.basicConfig(level=level)
    logger = logging.getLogger(__name__)
    return logger

def load_bronze_data(bronze_dir: str) -> pl.DataFrame:
    """Load all parquet files from bronze directory into a single DataFrame"""
    logger = logging.getLogger(__name__)
    
    # Find all parquet files in bronze directory
    parquet_pattern = os.path.join(bronze_dir, "**", "*.parquet")
    parquet_files = glob.glob(parquet_pattern, recursive=True)
    
    if not parquet_files:
        raise ValueError(f"No parquet files found in {bronze_dir}")
    
    logger.info(f"Found {len(parquet_files)} parquet files to process")
    # Read all parquet files into a single DataFrame
    for f in parquet_files: 
        df = pl.read_parquet(f)
        logger.info(f"Data Frame: {df}")
    #df = pl.concat([pl.read_parquet(f) for f in parquet_files])
    
    logger.info(f"Loaded {len(df):,} rows from bronze data")
    return df

def main():
    """Main processing pipeline"""
    
    parser = argparse.ArgumentParser(description="Process bronze data to silver layer")
    parser.add_argument("--bronze-dir", required=True, help="Path to bronze parquet files")
    parser.add_argument("--sample-size", type=int, help="Process only a sample of data for testing")
    parser.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (-v, -vv for more detail)"
    )
    args = parser.parse_args()
    
    logger = get_logger(args.verbose)
    logger.info("Starting silver layer processing...")
    
    # Load bronze data
    df = load_bronze_data(args.bronze_dir)

    #Print out data frame
    logger.debug(f"Data frame: {df}") 
    
    # Sample data if requested (for testing)
    if args.sample_size:
        df = df.sample(n=args.sample_size, seed=42)
        logger.info(f"Sampled {len(df):,} rows for testing")
    
    # Preprocess for ML
    df_processed = preprocess_for_ml(df)
    
    # Print summary
    logger.info("="*60)
    logger.info("SILVER PROCESSING SUMMARY")
    logger.info("="*60)
    logger.info(f"📄 Total rows processed: {len(df_processed):,}")
    logger.info(f"📊 Columns: {list(df_processed.columns)}")
    logger.info(f"💾 Memory usage: {df_processed.estimated_size('mb'):.2f} MB")
    
    if "text_length" in df_processed.columns:
        stats = df_processed.select([
            pl.col("text_length").mean().alias("avg_length"),
            pl.col("text_length").median().alias("median_length"),
            pl.col("text_length").min().alias("min_length"),
            pl.col("text_length").max().alias("max_length"),
        ])
        logger.info(f"📏 Text length stats: {stats.to_dicts()[0]}")
    
    logger.info("="*60)

if __name__ == "__main__":
    main()

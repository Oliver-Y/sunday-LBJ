import time
import argparse, os, bz2, requests, csv as _csv
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pyarrow.compute as pc
import logging

NEEDED_COLS = ["plain_text", "id", "author_id", "author_str", "type", "date_created","per_curiam"]

class DataStreamingStats:
    def __init__(self):
        self.download_time = 0 #Net time it takes to download from AWS
        self.bytes_streamed = 0
        self.bytes_saved = 0
        self.total_rows = 0
        self.total_rows_filtered = 0
        self.parquet_files_created = 0
        self.batches_processed = 0
        self.per_batch_time = []  #keep track of runing average of batches

    def add_bytes_saved(self, bytes_count):
        """Add to the total bytes saved through text normalization"""
        self.bytes_saved += bytes_count
        
    def get_space_saving(self):
        """Calculate the percentage of bytes saved through normalization"""
        if self.bytes_streamed == 0:
            return 0
        return (1 - ((self.bytes_streamed - self.bytes_saved) / self.bytes_streamed)) * 100
        
    def get_retention_rate(self):
        """Calculate the percentage of rows retained after filtering"""
        if self.total_rows == 0:
            return 0
        return (self.total_rows_filtered / self.total_rows) * 100
        
    def print_summary(self, logger):
        """Print a comprehensive summary of the data processing stats"""
        logger.info("\n" + "="*60)
        logger.info("📊 DATA PROCESSING SUMMARY")
        logger.info("="*60)
        
        # Per-batch timing statistics
        if self.per_batch_time:
            avg_batch_time = sum(self.per_batch_time) / len(self.per_batch_time)
            min_batch_time = min(self.per_batch_time)
            max_batch_time = max(self.per_batch_time)
            total_batch_time = sum(self.per_batch_time)
            
            logger.info(f"🔄 Batches Processed: {len(self.per_batch_time)}")
            logger.info(f"⏱️  Total Batch Time: {total_batch_time:.2f} seconds ({total_batch_time/60:.2f} minutes)")
            logger.info(f"⏱️  Avg Batch Time: {avg_batch_time:.3f} seconds")
            logger.info(f"⏱️  Min Batch Time: {min_batch_time:.3f} seconds")
            logger.info(f"⏱️  Max Batch Time: {max_batch_time:.3f} seconds")
            
            # Processing rate based on batch times
            if self.total_rows > 0 and total_batch_time > 0:
                rows_per_second = self.total_rows / total_batch_time
                logger.info(f"⚡ Processing Rate: {rows_per_second:,.0f} rows/second")
        
        # Download time
        if self.download_time > 0:
            logger.info(f"🌐 Download Time: {self.download_time:.2f} seconds")
        
        # Data processing stats
        logger.info(f"📦 Parquet Files Created: {self.parquet_files_created}")
        logger.info(f"📄 Total Rows: {self.total_rows:,}")
        logger.info(f"🗑️ Post-Filter Rows: {self.total_rows_filtered:,}")
        logger.info(f"📊 Retention Rate: {self.get_retention_rate():.2f}%")
        
        # Data size information
        logger.info(f"💾 Bytes Streamed: {self.bytes_streamed:,} ({self.bytes_streamed/1024/1024/1024:.2f} GB)")
        logger.info(f"💿 Bytes Saved: {self.bytes_saved:,} ({self.bytes_saved/1024/1024/1024:.2f} GB)")
        logger.info(f"📈 Space Saving: {self.get_space_saving():.2f}%")
        
        logger.info("="*60)



#Returns unzipped
def req_unzip_data(url, format:str, timeout:int): 
    r0 = requests.get(url, stream=True, timeout=timeout)
    r0.raise_for_status()
    bz0 = bz2.BZ2File(r0.raw)
    return bz0 

def get_logger(level_num:int): 
    level = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}.get(
        level_num, logging.DEBUG
    )
    logging.basicConfig(level=level)
    logger = logging.getLogger(__name__)
    return logger

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="CourtListener CSV.bz2 URL")
    ap.add_argument("--out-dir", required=True, help="Local output dir, e.g. data/bronze/opinions/2025-09-04")
    ap.add_argument("--rows-per-shard", type=int, default=2_000_000)
    ap.add_argument("--block-mb", type=int, default=128, help="CSV read block size (MiB)")
    ap.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (-v, -vv for more detail)"
    )
    args = ap.parse_args()
    # Initialize stats tracking
    stats = DataStreamingStats()
    start_time = time.time()
    
    logger = get_logger(args.verbose)
    os.makedirs(args.out_dir, exist_ok=True)
    data = req_unzip_data(args.url, "bzip", 600)
    logger.debug(f"Data: {data}")
    stats.download_time = time.time() - start_time

    read_opts = pacsv.ReadOptions(encoding="utf8", block_size=args.block_mb * 1024 * 1024)
    parse_opts = pacsv.ParseOptions(delimiter=",", quote_char='"', escape_char='\\', newlines_in_values=True)
    convert_opts = pacsv.ConvertOptions(
        include_columns=NEEDED_COLS,
        column_types={c: pa.string() for c in NEEDED_COLS},  
        strings_can_be_null=True
    )

    #Read the CSV - reuse the data from req_unzip_data
    reader = pacsv.open_csv(data, read_options=read_opts, parse_options=parse_opts, convert_options=convert_opts)
    logger.debug(f"Opening BZ file {data}")
    schema = pa.schema([pa.field(c, pa.string()) for c in NEEDED_COLS])

    def shard_path(i): return os.path.join(args.out_dir, f"part-{i:05d}.parquet")
    writer = pq.ParquetWriter(shard_path(0), schema=schema, compression="zstd")

    total = rows_in_shard = shard_i = 0
    try:
        prev_pull_t = time.perf_counter()
        for batch in reader:
            pulled_t = time.perf_counter()
            io_time = pulled_t - prev_pull_t
            proc_start = pulled_t


        
            tbl = pa.Table.from_batches([batch])
            logger.debug(f"Table: {tbl}")
            if tbl.num_rows == 0 or "plain_text" not in tbl.column_names:
                logger.warning("Error so skipping") 
                continue
            # Filter out rows where plain_text is None or empty
            plain_text_col = tbl.column("plain_text")
            valid_mask = pa.compute.and_(
                pa.compute.is_valid(plain_text_col),  # not None
                pa.compute.greater(pa.compute.utf8_length(plain_text_col), 0)  # not empty
            )
            filtered_tbl = tbl.filter(valid_mask)
            
            if filtered_tbl.num_rows == 0:
                logger.info("📭 Batch filtered out - no valid plain_text content")
                continue
                
            # Normalize the text
            plain_text = filtered_tbl["plain_text"]
            logger.debug(f"Un-normalized plain_text {plain_text}")
            normalized = pc.replace_substring_regex(plain_text, r"\s+", " ")
            normalized = pc.utf8_trim_whitespace(normalized)
            logger.debug(f"Normalize plain_text {normalized}")
            # Calculate bytes saved through normalization
            original_size = sum(len(str(text)) for text in plain_text.to_pylist())
            normalized_size = sum(len(str(text)) for text in normalized.to_pylist())
            bytes_saved = original_size - normalized_size

            #Update stats
            stats.bytes_streamed += original_size
            stats.bytes_saved += bytes_saved
            stats.total_rows += tbl.num_rows  # Only count the good rows
            stats.total_rows_filtered += filtered_tbl.num_rows  # Count the filtered out rows
            
            # Replace the plain_text column with normalized text
            normalized_tbl = filtered_tbl.set_column(0, "plain_text", normalized)
            logger.debug(f"Processing batch: {tbl.num_rows:,} → {filtered_tbl.num_rows:,} rows \n Bytes saved: {bytes_saved}")
            
            writer.write_table(normalized_tbl)
            total += filtered_tbl.num_rows
            rows_in_shard += filtered_tbl.num_rows
            if rows_in_shard >= args.rows_per_shard:
                writer.close()
                shard_i += 1
                rows_in_shard = 0
                writer = pq.ParquetWriter(shard_path(shard_i), schema=schema, compression="zstd")
                stats.parquet_files_created += 1
                
            proc_time = time.perf_counter() - proc_start
            batch_total = io_time + proc_time
            stats.per_batch_time.append(batch_total)
            stats.batches_processed += 1
            if stats.batches_processed % 25 == 0:
                stats.print_summary(logger)
            else:
                logger.info("Batch %d: time=%.3fs rows=%s",
                            stats.batches_processed,
                            batch_total,
                            f"{filtered_tbl.num_rows:,}")
            prev_pull_t = time.perf_counter()
    
    finally:
        writer.close()
        
    # Final summary
    logger.info("🏁 Processing Complete!")


if __name__ == "__main__":
    main()

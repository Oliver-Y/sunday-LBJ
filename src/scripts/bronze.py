# csvbz2_plaintext_to_parquet.py
import argparse, os, bz2, requests, csv as _csv
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="CourtListener CSV.bz2 URL")
    ap.add_argument("--out-dir", required=True, help="Local output dir, e.g. data/bronze/opinions/2025-09-04")
    ap.add_argument("--rows-per-shard", type=int, default=2_000_000)
    ap.add_argument("--block-mb", type=int, default=128, help="CSV read block size (MiB)")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    r0 = requests.get(args.url, stream=True, timeout=60); r0.raise_for_status()
    bz0 = bz2.BZ2File(r0.raw)
    header = bz0.readline().decode("utf-8", errors="replace").rstrip("\n")
    cols = next(_csv.reader([header], delimiter=",", quotechar='"', escapechar='\\'))

    needed = ["plain_text", "id", "author_id", "type"] 
    print(f"Cols: {cols}")
    missing = [c for c in needed if c not in cols]
    if missing:
        raise SystemExit(f"Missing expected columns: {missing}")

    # 2) CSV reader—only the columns you want, big block for multi-line cells
    read_opts = pacsv.ReadOptions(encoding="utf8", block_size=args.block_mb * 1024 * 1024)
    parse_opts = pacsv.ParseOptions(delimiter=",", quote_char='"', escape_char='\\', newlines_in_values=True)
    convert_opts = pacsv.ConvertOptions(
        include_columns=needed,
        column_types={c: pa.string() for c in needed},  # force strings
        strings_can_be_null=True
    )

    r = requests.get(args.url, stream=True, timeout=60); r.raise_for_status()
    bz = bz2.BZ2File(r.raw)
    reader = pacsv.open_csv(bz, read_options=read_opts, parse_options=parse_opts, convert_options=convert_opts)

    # 3) Parquet shards
    schema = pa.schema([pa.field(c, pa.string()) for c in needed])
    def shard_path(i): return os.path.join(args.out_dir, f"part-{i:05d}.parquet")
    writer = pq.ParquetWriter(shard_path(0), schema=schema, compression="zstd")

    total = rows_in_shard = shard_i = 0
    try:
        for batch in reader:
            #print(f"Batch: {batch}")
            tbl = pa.Table.from_batches([batch])
            if tbl.num_rows == 0:
                continue
            writer.write_table(tbl)
            total += tbl.num_rows
            rows_in_shard += tbl.num_rows
            if rows_in_shard >= args.rows_per_shard:
                writer.close()
                shard_i += 1
                rows_in_shard = 0
                writer = pq.ParquetWriter(shard_path(shard_i), schema=schema, compression="zstd")
    finally:
        writer.close()
    print(f"Wrote {shard_i+1} shard(s), {total:,} rows → {args.out_dir}")

if __name__ == "__main__":
    main()

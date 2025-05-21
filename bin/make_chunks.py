#!/usr/bin/env python3

import argparse
from pathlib import Path

import polars as pl


def make_parser():
    parser = argparse.ArgumentParser(
        description="Make chunks of the data for the chromosome 21 dataset."
    )
    parser.add_argument(
        "--parquet",
        type=str,
        help="Base path to the data.",
    )
    parser.add_argument(
        "--size",
        type=int,
        default=1000,
        help="Size of the chunks to make.",
    )
    parser.add_argument(
        "--stride",
        type=int,
        default=1000,
        help="Stride of the chunks to make.",
    )
    parser.add_argument(
        "--outpath",
        type=str,
        default=".",
        help="Directory to write the output files to.",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="myseq",
        help="Prefix for the output files.",
    )
    return parser


def main():
    parser = make_parser()
    args = parser.parse_args()
    
    # Read the parquet file
    df_seq = pl.read_parquet(args.parquet)

    # Get the first row of the dataframe
    seq = df_seq.item(0,2)

    chunks = []

    # Create chunks of the sequence
    for start in range(0,len(seq),args.stride):
        chunk_seq = seq[start:start+args.size]
        chunks.append({
            "id": df_seq.item(0,0),
            "start": start,
            "end": start+args.size,
            "sequence": chunk_seq
        })

    # Create a dataframe from the chunks
    df_chunks = pl.DataFrame(chunks)

    # Write the dataframe to a parquet file
    df_chunks.write_parquet(Path(args.outpath) / f"{args.prefix}_chunks_{args.size}.pq")

    print(f"Chunks written to {args.outpath}/{args.prefix}_chunks_{args.size}.pq")


if __name__ == "__main__":
    main()


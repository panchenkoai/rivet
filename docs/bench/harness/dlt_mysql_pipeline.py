"""dlt pipeline: extract one MySQL table to local Parquet (Snappy)."""

import os
import sys

import dlt
from dlt.sources.sql_database import sql_table


def run(table_name: str, output_root: str, conn_url: str) -> int:
    os.environ["NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION"] = "true"
    os.environ["DATA_WRITER__FILE_FORMAT"] = "parquet"
    os.environ["NORMALIZE__DATA_WRITER__FILE_FORMAT"] = "parquet"

    table_root = os.path.join(output_root, table_name)
    os.makedirs(table_root, exist_ok=True)

    pipeline = dlt.pipeline(
        pipeline_name=f"bench_my_{table_name}",
        destination=dlt.destinations.filesystem(bucket_url=f"file://{table_root}"),
        dataset_name="rivet_compete",
        progress=None,
    )
    source = sql_table(
        credentials=conn_url,
        schema="rivet",
        table=table_name,
        backend="pyarrow",
        chunk_size=10000,
    )
    pipeline.run(source, loader_file_format="parquet")
    return 0


if __name__ == "__main__":
    sys.exit(run(sys.argv[1], sys.argv[2], sys.argv[3]))

from __future__ import annotations

import argparse
import math
import time
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.flight as flight


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark one running WendaoArrow packaged Flight listener over pyarrow.flight.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--samples", type=int, default=8)
    parser.add_argument("--request-rows", type=int, default=32)
    parser.add_argument("--deadline", type=float, default=30.0)
    return parser.parse_args()


def percentile(values: list[float], probability: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = max(0, min(len(sorted_values) - 1, math.ceil(probability * len(sorted_values)) - 1))
    return sorted_values[index]


def median(values: list[float]) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2:
        return sorted_values[midpoint]
    return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2.0


def request_table(rows: int) -> pa.Table:
    table = pa.table(
        {
            "doc_id": [f"doc-{index}" for index in range(1, rows + 1)],
            "vector_score": [0.25 + (index * 0.01) for index in range(rows)],
        }
    )
    schema = table.schema.with_metadata({b"wendao.schema_version": b"v1"})
    return table.cast(schema)


def table_ipc_bytes(table: pa.Table) -> int:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().size


def exchange_table(host: str, port: int, table: pa.Table, deadline: float) -> pa.Table:
    client = flight.connect(f"grpc://{host}:{port}")
    options = flight.FlightCallOptions(
        timeout=deadline,
        headers=[(b"x-wendao-schema-version", b"v1")],
    )
    writer, reader = client.do_exchange(flight.FlightDescriptor.for_path("rerank"), options)
    begin = getattr(writer, "begin", None)
    if callable(begin):
        begin(table.schema)
    writer.write_table(table)
    done_writing = getattr(writer, "done_writing", None)
    if callable(done_writing):
        done_writing()
    response = reader.read_all()
    close = getattr(client, "close", None)
    if callable(close):
        close()
    return response


def worker(host: str, port: int, request: pa.Table, samples: int, deadline: float) -> dict[str, object]:
    latencies_ms: list[float] = []
    failure_count = 0
    first_failure = ""
    response_bytes_per_request = 0
    response_rows = 0
    response_columns = 0

    for _ in range(samples):
        started = time.perf_counter_ns()
        try:
            response = exchange_table(host, port, request, deadline)
            latencies_ms.append((time.perf_counter_ns() - started) / 1_000_000.0)
            if response_bytes_per_request == 0:
                response_bytes_per_request = table_ipc_bytes(response)
                response_rows = response.num_rows
                response_columns = response.num_columns
        except Exception as error:  # noqa: BLE001
            failure_count += 1
            if not first_failure:
                first_failure = f"{type(error).__name__}: {error}"

    return {
        "latencies_ms": latencies_ms,
        "failure_count": failure_count,
        "first_failure": first_failure,
        "response_bytes_per_request": response_bytes_per_request,
        "response_rows": response_rows,
        "response_columns": response_columns,
    }


def first_positive(results: list[dict[str, object]], field: str) -> int:
    for result in results:
        value = int(result[field])
        if value > 0:
            return value
    return 0


def first_nonempty(results: list[dict[str, object]], field: str) -> str:
    for result in results:
        value = str(result[field])
        if value:
            return value
    return ""


def main() -> int:
    args = parse_args()
    if not args.host.strip():
        raise ValueError("benchmark host must be non-empty")
    if args.port <= 0:
        raise ValueError("benchmark port must be greater than zero")
    if args.workers <= 0:
        raise ValueError("benchmark workers must be greater than zero")
    if args.samples <= 0:
        raise ValueError("benchmark samples must be greater than zero")
    if args.request_rows <= 0:
        raise ValueError("benchmark request_rows must be greater than zero")
    if args.deadline <= 0:
        raise ValueError("benchmark deadline must be greater than zero")

    request = request_table(args.request_rows)
    started = time.perf_counter_ns()
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(worker, args.host, args.port, request, args.samples, args.deadline)
            for _ in range(args.workers)
        ]
        results = [future.result() for future in futures]
    elapsed_s = (time.perf_counter_ns() - started) / 1_000_000_000.0

    latencies_ms = [latency for result in results for latency in result["latencies_ms"]]
    success_count = len(latencies_ms)
    failure_count = sum(int(result["failure_count"]) for result in results)
    response_bytes_per_request = first_positive(results, "response_bytes_per_request")
    response_rows = first_positive(results, "response_rows")
    response_columns = first_positive(results, "response_columns")
    first_failure = first_nonempty(results, "first_failure")
    attempts = args.workers * args.samples
    total_response_bytes = response_bytes_per_request * success_count
    ops_per_sec = 0.0 if success_count == 0 else success_count / elapsed_s
    throughput_mib_per_sec = 0.0 if total_response_bytes == 0 else (total_response_bytes / elapsed_s) / (2.0**20)

    print(
        "\t".join(
            (
                "attempts",
                "success_count",
                "failure_count",
                "elapsed_s",
                "median_ms",
                "p95_ms",
                "p99_ms",
                "min_ms",
                "max_ms",
                "ops_per_sec",
                "throughput_mib_per_sec",
                "request_rows",
                "response_rows",
                "response_columns",
                "response_bytes_per_request",
                "first_failure",
            )
        )
    )
    print(
        "\t".join(
            (
                str(attempts),
                str(success_count),
                str(failure_count),
                f"{elapsed_s:.6f}",
                f"{median(latencies_ms):.3f}",
                f"{percentile(latencies_ms, 0.95):.3f}",
                f"{percentile(latencies_ms, 0.99):.3f}",
                f"{min(latencies_ms) if latencies_ms else 0.0:.3f}",
                f"{max(latencies_ms) if latencies_ms else 0.0:.3f}",
                f"{ops_per_sec:.3f}",
                f"{throughput_mib_per_sec:.3f}",
                str(args.request_rows),
                str(response_rows),
                str(response_columns),
                str(response_bytes_per_request),
                first_failure.replace("\t", " "),
            )
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

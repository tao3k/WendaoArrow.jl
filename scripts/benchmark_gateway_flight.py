from __future__ import annotations

import argparse
import time

import pyarrow.flight as flight

DEFAULT_HOST = "127.0.0.1"
DEFAULT_GATEWAY_FLIGHT_PORT = 9517
DEFAULT_GATEWAY_SCHEMA_VERSION = "v2"
DEFAULT_GATEWAY_RESULT_LIMIT = 10
DEFAULT_GATEWAY_REPO_SEARCH_ROUTE = ("search", "repos", "main")
DEFAULT_GATEWAY_KNOWLEDGE_SEARCH_ROUTE = ("search", "knowledge")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark one running Rust gateway Flight surface over pyarrow.flight.",
    )
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_GATEWAY_FLIGHT_PORT)
    parser.add_argument("--query", default="flight")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument("--route", choices=("repo", "knowledge", "both"), default="both")
    return parser.parse_args()


def median(values: list[float]) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2:
        return sorted_values[midpoint]
    return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2.0


def base_headers() -> list[tuple[bytes, bytes]]:
    return [(b"x-wendao-schema-version", DEFAULT_GATEWAY_SCHEMA_VERSION.encode("utf-8"))]


def repo_headers(query: str, limit: int) -> list[tuple[bytes, bytes]]:
    return [
        *base_headers(),
        (b"x-wendao-repo-search-query", query.encode("utf-8")),
        (b"x-wendao-repo-search-limit", str(limit).encode("utf-8")),
    ]


def knowledge_headers(query: str, limit: int) -> list[tuple[bytes, bytes]]:
    return [
        *base_headers(),
        (b"x-wendao-search-query", query.encode("utf-8")),
        (b"x-wendao-search-limit", str(limit).encode("utf-8")),
    ]


def call_options(headers: list[tuple[bytes, bytes]]) -> flight.FlightCallOptions:
    return flight.FlightCallOptions(timeout=30.0, headers=headers)


def read_route_table(
    client: flight.FlightClient,
    route_segments: tuple[str, ...],
    headers: list[tuple[bytes, bytes]],
):
    descriptor = flight.FlightDescriptor.for_path(*route_segments)
    options = call_options(headers)
    info = client.get_flight_info(descriptor, options)
    endpoint = info.endpoints[0]
    reader = client.do_get(endpoint.ticket, options)
    return reader.read_all()


def bench_case(
    client: flight.FlightClient,
    *,
    label: str,
    route_segments: tuple[str, ...],
    headers: list[tuple[bytes, bytes]],
    samples: int,
) -> dict[str, object]:
    warm_table = read_route_table(client, route_segments, headers)
    times_ms: list[float] = []
    for _ in range(samples):
        started = time.perf_counter_ns()
        _ = read_route_table(client, route_segments, headers)
        times_ms.append((time.perf_counter_ns() - started) / 1_000_000.0)
    return {
        "case": label,
        "median_ms": median(times_ms),
        "minimum_ms": min(times_ms),
        "maximum_ms": max(times_ms),
        "rows": warm_table.num_rows,
        "columns": warm_table.num_columns,
    }


def print_results(results: list[dict[str, object]]) -> None:
    print("case\tmedian_ms\tminimum_ms\tmaximum_ms\trows\tcolumns")
    for result in results:
        print(
            "\t".join(
                (
                    str(result["case"]),
                    f"{float(result['median_ms']):.3f}",
                    f"{float(result['minimum_ms']):.3f}",
                    f"{float(result['maximum_ms']):.3f}",
                    str(result["rows"]),
                    str(result["columns"]),
                )
            )
        )


def main() -> int:
    args = parse_args()
    if not args.host.strip():
        raise ValueError("benchmark host must be non-empty")
    if args.port <= 0:
        raise ValueError("benchmark port must be greater than zero")
    if not args.query.strip():
        raise ValueError("benchmark query must be non-empty")
    if args.limit <= 0:
        raise ValueError("benchmark limit must be greater than zero")
    if args.samples <= 0:
        raise ValueError("benchmark samples must be greater than zero")

    client = flight.connect(f"grpc://{args.host}:{args.port}")
    results: list[dict[str, object]] = []
    if args.route in ("repo", "both"):
        results.append(
            bench_case(
                client,
                label="repo_search",
                route_segments=DEFAULT_GATEWAY_REPO_SEARCH_ROUTE,
                headers=repo_headers(args.query, args.limit),
                samples=args.samples,
            )
        )
    if args.route in ("knowledge", "both"):
        results.append(
            bench_case(
                client,
                label="knowledge_search",
                route_segments=DEFAULT_GATEWAY_KNOWLEDGE_SEARCH_ROUTE,
                headers=knowledge_headers(args.query, args.limit),
                samples=args.samples,
            )
        )
    close = getattr(client, "close", None)
    if callable(close):
        close()
    print_results(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

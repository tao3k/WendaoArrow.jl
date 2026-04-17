include(
    joinpath(@__DIR__, "..", "..", "scripts", "run_packaged_flight_benchmark_server.jl"),
)

const PackagedFlightBenchServer = WendaoArrowPackagedFlightBenchmarkServer

@testset "Packaged benchmark server arguments are parsed explicitly" begin
    config = PackagedFlightBenchServer.parse_server_args([
        "--host",
        "127.0.0.1",
        "--port",
        "0",
        "--response-mode",
        "large_response",
        "--large-doc-bytes",
        "65536",
        "--processing-delay-ms",
        "5",
        "--max-active-requests",
        "7",
        "--request-capacity",
        "8",
        "--response-capacity",
        "9",
    ])

    @test config.host == "127.0.0.1"
    @test config.port == 0
    @test config.response_mode == :large_response
    @test config.large_doc_bytes == 65536
    @test config.processing_delay_ms == 5
    @test config.max_active_requests == 7
    @test config.request_capacity == 8
    @test config.response_capacity == 9

    @test_throws ArgumentError PackagedFlightBenchServer.parse_server_args([
        "--response-mode",
        "bad",
    ])
    @test_throws ArgumentError PackagedFlightBenchServer.parse_server_args([
        "--request-capacity",
        "0",
    ])
    @test_throws ArgumentError PackagedFlightBenchServer.parse_server_args(["--unknown"])
end

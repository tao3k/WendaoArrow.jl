using Test
using HTTP
using Tables
using WendaoArrow

function sample_table()
    return (
        doc_id = ["doc-a", "doc-b"],
        vector_score = [0.9, 0.5],
    )
end

@testset "Arrow IPC roundtrip" begin
    bytes = WendaoArrow.encode_ipc(sample_table())
    table = WendaoArrow.decode_ipc(bytes)
    columns = Tables.columntable(table)

    @test columns.doc_id == ["doc-a", "doc-b"]
    @test columns.vector_score == [0.9, 0.5]
end

@testset "HTTP handler delegates processing" begin
    handler = WendaoArrow.build_handler(
        config = WendaoArrow.InterfaceConfig(
            route = WendaoArrow.DEFAULT_ROUTE,
            health_route = WendaoArrow.DEFAULT_HEALTH_ROUTE,
        ),
    ) do table
        columns = Tables.columntable(table)
        return (
            doc_id = collect(columns.doc_id),
            passthrough_score = Float64.(columns.vector_score),
        )
    end

    request = HTTP.Request(
        "POST",
        WendaoArrow.DEFAULT_ROUTE,
        ["Content-Type" => WendaoArrow.CONTENT_TYPE],
        WendaoArrow.encode_ipc(sample_table()),
    )
    response = handler(request)
    result = Tables.columntable(WendaoArrow.decode_ipc(response.body))

    @test response.status == 200
    @test result.doc_id == ["doc-a", "doc-b"]
    @test result.passthrough_score == [0.9, 0.5]
end

@testset "HTTP handler rejects wrong route" begin
    handler = WendaoArrow.build_handler(
        identity;
        config = WendaoArrow.InterfaceConfig(
            route = WendaoArrow.DEFAULT_ROUTE,
            health_route = WendaoArrow.DEFAULT_HEALTH_ROUTE,
        ),
    )
    request = HTTP.Request(
        "POST",
        "/wrong-route",
        ["Content-Type" => WendaoArrow.CONTENT_TYPE],
        WendaoArrow.encode_ipc(sample_table()),
    )
    response = handler(request)

    @test response.status == 404
end

@testset "HTTP handler serves health route" begin
    handler = WendaoArrow.build_handler(
        identity;
        config = WendaoArrow.InterfaceConfig(
            route = WendaoArrow.DEFAULT_ROUTE,
            health_route = WendaoArrow.DEFAULT_HEALTH_ROUTE,
        ),
    )
    request = HTTP.Request("GET", WendaoArrow.DEFAULT_HEALTH_ROUTE)
    response = handler(request)

    @test response.status == 200
    @test String(response.body) == "{\"status\":\"ok\"}"
end

@testset "HTTP handler rejects invalid Arrow payload" begin
    handler = WendaoArrow.build_handler(
        identity;
        config = WendaoArrow.InterfaceConfig(
            route = WendaoArrow.DEFAULT_ROUTE,
            health_route = WendaoArrow.DEFAULT_HEALTH_ROUTE,
        ),
    )
    request = HTTP.Request(
        "POST",
        WendaoArrow.DEFAULT_ROUTE,
        ["Content-Type" => WendaoArrow.CONTENT_TYPE],
        codeunits("not-arrow-ipc"),
    )
    response = handler(request)

    @test response.status == 400
    @test String(response.body) == "{\"error\":\"invalid_arrow_ipc_request\"}"
end

@testset "HTTP handler catches processor failures" begin
    handler = WendaoArrow.build_handler(
        _ -> error("boom");
        config = WendaoArrow.InterfaceConfig(
            route = WendaoArrow.DEFAULT_ROUTE,
            health_route = WendaoArrow.DEFAULT_HEALTH_ROUTE,
        ),
    )
    request = HTTP.Request(
        "POST",
        WendaoArrow.DEFAULT_ROUTE,
        ["Content-Type" => WendaoArrow.CONTENT_TYPE],
        WendaoArrow.encode_ipc(sample_table()),
    )
    response = handler(request)

    @test response.status == 500
    @test String(response.body) == "{\"error\":\"processor_failed\"}"
end

@testset "config loading merges TOML and flags" begin
    config_path = tempname() * ".toml"
    write(
        config_path,
        """
        [interface]
        host = "0.0.0.0"
        port = 18080
        route = "/from-toml"
        health_route = "/health-from-toml"
        content_type = "application/test-arrow"
        """,
    )

    config = WendaoArrow.config_from_args([
        "--config",
        config_path,
        "--port",
        "19090",
        "--route=/from-flag",
        "--health-route",
        "/health-from-flag",
    ])

    @test config.host == "0.0.0.0"
    @test config.port == 19090
    @test config.route == "/from-flag"
    @test config.health_route == "/health-from-flag"
    @test config.content_type == "application/test-arrow"
end

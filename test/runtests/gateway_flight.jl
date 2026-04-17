@testset "Generic Flight descriptor normalizes route segments" begin
    @test WendaoArrow.flight_route_descriptor("/graph/structural/rerank").path ==
          ["graph", "structural", "rerank"]
    @test WendaoArrow.flight_route_descriptor("graph/structural/filter").path ==
          ["graph", "structural", "filter"]
    @test_throws ArgumentError WendaoArrow.flight_route_descriptor("")
    @test_throws ArgumentError WendaoArrow.flight_route_descriptor("/")
end

@testset "Generic Flight schema headers follow shared contract" begin
    headers = Dict(
        WendaoArrow.flight_schema_headers(
            schema_version = "v0-draft",
            headers = ["x-trace-id" => "trace-0"],
        ),
    )

    @test headers["x-wendao-schema-version"] == "v0-draft"
    @test headers["x-trace-id"] == "trace-0"

    default_headers = Dict(WendaoArrow.flight_schema_headers())
    @test default_headers["x-wendao-schema-version"] == WendaoArrow.DEFAULT_SCHEMA_VERSION

    @test_throws ArgumentError WendaoArrow.flight_schema_headers(schema_version = "")
end

@testset "Gateway Flight descriptor normalizes route segments" begin
    @test WendaoArrow.gateway_flight_descriptor("/search/repos/main").path ==
          ["search", "repos", "main"]
    @test WendaoArrow.gateway_flight_descriptor("search/knowledge").path ==
          ["search", "knowledge"]
    @test_throws ArgumentError WendaoArrow.gateway_flight_descriptor("")
    @test_throws ArgumentError WendaoArrow.gateway_flight_descriptor("/")
end

@testset "Gateway repo search headers follow runtime contract" begin
    headers = Dict(
        WendaoArrow.gateway_repo_search_headers(
            "flight";
            limit = 5,
            headers = ["x-trace-id" => "trace-1"],
        ),
    )

    @test headers["x-wendao-schema-version"] == WendaoArrow.DEFAULT_GATEWAY_SCHEMA_VERSION
    @test headers["x-wendao-repo-search-query"] == "flight"
    @test headers["x-wendao-repo-search-limit"] == "5"
    @test headers["x-trace-id"] == "trace-1"

    @test_throws ArgumentError WendaoArrow.gateway_repo_search_headers("")
    @test_throws ArgumentError WendaoArrow.gateway_repo_search_headers("flight"; limit = 0)
end

@testset "Gateway knowledge search headers follow runtime contract" begin
    headers = Dict(
        WendaoArrow.gateway_knowledge_search_headers(
            "flight";
            limit = 7,
            intent = "code-search",
            repo = "alpha/repo",
            headers = ["x-trace-id" => "trace-2"],
        ),
    )

    @test headers["x-wendao-schema-version"] == WendaoArrow.DEFAULT_GATEWAY_SCHEMA_VERSION
    @test headers["x-wendao-search-query"] == "flight"
    @test headers["x-wendao-search-limit"] == "7"
    @test headers["x-wendao-search-intent"] == "code-search"
    @test headers["x-wendao-search-repo"] == "alpha/repo"
    @test headers["x-trace-id"] == "trace-2"

    @test_throws ArgumentError WendaoArrow.gateway_knowledge_search_headers("")
    @test_throws ArgumentError WendaoArrow.gateway_knowledge_search_headers(
        "flight";
        repo = "",
    )
end

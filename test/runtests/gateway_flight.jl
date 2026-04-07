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

@testset "Gateway Flight client validates transport config" begin
    @test_throws ArgumentError WendaoArrow.gateway_flight_client(; host = "", port = 9517)
    @test_throws ArgumentError WendaoArrow.gateway_flight_client(;
        host = "127.0.0.1",
        port = 0,
    )
    @test_throws ArgumentError WendaoArrow.gateway_flight_client(;
        host = "127.0.0.1",
        port = 9517,
        deadline = 0,
    )
end

if get(ENV, "WENDAO_GATEWAY_FLIGHT_SMOKE", "0") == "1"
    @testset "Gateway Flight live smoke" begin
        address = get(ENV, "WENDAO_GATEWAY_FLIGHT_ADDR", "127.0.0.1:9517")
        parts = split(address, ":")
        length(parts) == 2 ||
            error("WENDAO_GATEWAY_FLIGHT_ADDR must be host:port; got $(address)")
        host, port_text = parts
        port = parse(Int, port_text)
        client =
            WendaoArrow.gateway_flight_client(; host = host, port = port, deadline = 30)

        repo_table = WendaoArrow.gateway_repo_search(client, "flight"; limit = 5)
        repo_schema = Tables.schema(repo_table)
        @test repo_schema.names == (
            :doc_id,
            :path,
            :title,
            :best_section,
            :match_reason,
            :navigation_path,
            :navigation_category,
            :navigation_line,
            :navigation_line_end,
            :hierarchy,
            :tags,
            :score,
            :language,
        )
        @test length(Tables.rows(repo_table)) >= 0

        knowledge_table = WendaoArrow.gateway_knowledge_search(client, "flight"; limit = 5)
        knowledge_schema = Tables.schema(knowledge_table)
        @test knowledge_schema.names == (
            :stem,
            :title,
            :path,
            :docType,
            :tagsJson,
            :score,
            :bestSection,
            :matchReason,
            :hierarchicalUri,
            :hierarchyJson,
            :saliencyScore,
            :auditStatus,
            :verificationState,
            :implicitBacklinksJson,
            :implicitBacklinkItemsJson,
            :navigationTargetJson,
        )
        @test length(Tables.rows(knowledge_table)) >= 0
    end
end

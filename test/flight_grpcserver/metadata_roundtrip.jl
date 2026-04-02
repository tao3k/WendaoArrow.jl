@testset "cross-process metadata Flight startup script serves pyarrow DoExchange with additive metadata response columns" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_metadata_doexchange_command(
                python,
                port;
                trace_id = "trace-123",
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_hit = true,
                cache_score = 0.75,
                cache_generated_at = "2026-03-30T09:00:00",
                cache_backend = "remote",
                cache_scope = "tenant",
                ranking_strategy = "hybrid",
                retrieval_mode = "hybrid",
            ),
        )
        @test chomp(output) ==
              "{\"analyzer_score\": [0.9, 0.5], \"attempt_count\": [3, 3], \"attempt_count_field_metadata\": {\"semantic.role\": \"attempt-count\"}, \"cache_backend\": [3, 3], \"cache_backend_extension_metadata\": \"$(CACHE_BACKEND_EXTENSION_METADATA)\", \"cache_backend_extension_name\": \"$(CACHE_BACKEND_EXTENSION_NAME)\", \"cache_backend_field_metadata\": {\"ARROW:extension:metadata\": \"$(CACHE_BACKEND_EXTENSION_METADATA)\", \"ARROW:extension:name\": \"$(CACHE_BACKEND_EXTENSION_NAME)\", \"semantic.role\": \"cache-backend\"}, \"cache_generated_at\": [\"2026-03-30T09:00:00\", \"2026-03-30T09:00:00\"], \"cache_hit\": [true, true], \"cache_scope\": [2, 2], \"cache_scope_extension_metadata\": \"$(CACHE_SCOPE_EXTENSION_METADATA)\", \"cache_scope_extension_name\": \"$(CACHE_SCOPE_EXTENSION_NAME)\", \"cache_score\": [0.75, 0.75], \"doc_id\": [\"doc-a\", \"doc-b\"], \"final_score\": [0.9, 0.5], \"ranking_strategy\": [3, 3], \"ranking_strategy_extension_metadata\": \"$(RANKING_STRATEGY_EXTENSION_METADATA)\", \"ranking_strategy_extension_name\": \"$(RANKING_STRATEGY_EXTENSION_NAME)\", \"ranking_strategy_field_metadata\": {\"ARROW:extension:metadata\": \"$(RANKING_STRATEGY_EXTENSION_METADATA)\", \"ARROW:extension:name\": \"$(RANKING_STRATEGY_EXTENSION_NAME)\", \"semantic.role\": \"ranking-strategy\"}, \"retrieval_mode\": [2, 2], \"retrieval_mode_extension_metadata\": \"$(RETRIEVAL_MODE_EXTENSION_METADATA)\", \"retrieval_mode_extension_name\": \"$(RETRIEVAL_MODE_EXTENSION_NAME)\", \"tenant_id\": [\"tenant-7\", \"tenant-7\"], \"trace_id\": [\"trace-123\", \"trace-123\"], \"trace_id_field_metadata\": {\"semantic.role\": \"trace-id\"}}"
    end
end

@testset "cross-process metadata Flight startup script serves native Julia DoExchange with additive metadata response columns" begin
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        response_table = native_julia_doexchange_table(
            port;
            metadata = metadata_request_metadata(
                "trace-123";
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_hit = true,
                cache_score = 0.75,
                cache_generated_at = "2026-03-30T09:00:00",
                cache_backend = "remote",
                cache_scope = "tenant",
                ranking_strategy = "hybrid",
                retrieval_mode = "hybrid",
            ),
        )
        response = Tables.columntable(response_table)
        assert_metadata_scoring_columns(
            response,
            Union{Missing,String}["trace-123", "trace-123"],
            Union{Missing,String}["tenant-7", "tenant-7"],
            Union{Missing,Int64}[3, 3],
            Union{Missing,WendaoArrow.CacheBackend}[WendaoArrow.remote, WendaoArrow.remote],
            Union{Missing,WendaoArrow.CacheScope}[WendaoArrow.tenant, WendaoArrow.tenant],
            Union{Missing,WendaoArrow.RankingStrategy}[
                WendaoArrow.hybrid,
                WendaoArrow.hybrid,
            ],
            Union{Missing,WendaoArrow.LinkGraphRetrievalMode}[
                WendaoArrow.LinkGraphRetrievalModes.hybrid,
                WendaoArrow.LinkGraphRetrievalModes.hybrid,
            ],
            Union{Missing,Bool}[true, true],
            Union{Missing,Float64}[0.75, 0.75],
            Union{Missing,DateTime}[
                DateTime(2026, 3, 30, 9, 0, 0),
                DateTime(2026, 3, 30, 9, 0, 0),
            ],
        )
        @test Arrow.getmetadata(Tables.getcolumn(response_table, :trace_id))["semantic.role"] ==
              "trace-id"
        @test Arrow.getmetadata(Tables.getcolumn(response_table, :attempt_count))["semantic.role"] ==
              "attempt-count"
        @test Arrow.getmetadata(Tables.getcolumn(response_table, :cache_backend))["semantic.role"] ==
              "cache-backend"
        @test Arrow.getmetadata(Tables.getcolumn(response_table, :ranking_strategy))["semantic.role"] ==
              "ranking-strategy"
    end
end

@testset "cross-process metadata Flight startup script serves pyarrow DoExchange with missing additive metadata response columns" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(pyarrow_metadata_doexchange_command(python, port))
        @test chomp(output) ==
              "{\"analyzer_score\": [0.9, 0.5], \"attempt_count\": [null, null], \"attempt_count_field_metadata\": {\"semantic.role\": \"attempt-count\"}, \"cache_backend\": [null, null], \"cache_backend_extension_metadata\": \"$(CACHE_BACKEND_EXTENSION_METADATA)\", \"cache_backend_extension_name\": \"$(CACHE_BACKEND_EXTENSION_NAME)\", \"cache_backend_field_metadata\": {\"ARROW:extension:metadata\": \"$(CACHE_BACKEND_EXTENSION_METADATA)\", \"ARROW:extension:name\": \"$(CACHE_BACKEND_EXTENSION_NAME)\", \"semantic.role\": \"cache-backend\"}, \"cache_generated_at\": [null, null], \"cache_hit\": [null, null], \"cache_scope\": [null, null], \"cache_scope_extension_metadata\": \"$(CACHE_SCOPE_EXTENSION_METADATA)\", \"cache_scope_extension_name\": \"$(CACHE_SCOPE_EXTENSION_NAME)\", \"cache_score\": [null, null], \"doc_id\": [\"doc-a\", \"doc-b\"], \"final_score\": [0.9, 0.5], \"ranking_strategy\": [null, null], \"ranking_strategy_extension_metadata\": \"$(RANKING_STRATEGY_EXTENSION_METADATA)\", \"ranking_strategy_extension_name\": \"$(RANKING_STRATEGY_EXTENSION_NAME)\", \"ranking_strategy_field_metadata\": {\"ARROW:extension:metadata\": \"$(RANKING_STRATEGY_EXTENSION_METADATA)\", \"ARROW:extension:name\": \"$(RANKING_STRATEGY_EXTENSION_NAME)\", \"semantic.role\": \"ranking-strategy\"}, \"retrieval_mode\": [null, null], \"retrieval_mode_extension_metadata\": \"$(RETRIEVAL_MODE_EXTENSION_METADATA)\", \"retrieval_mode_extension_name\": \"$(RETRIEVAL_MODE_EXTENSION_NAME)\", \"tenant_id\": [null, null], \"trace_id\": [null, null], \"trace_id_field_metadata\": {\"semantic.role\": \"trace-id\"}}"
    end
end

@testset "cross-process metadata Flight startup script serves native Julia DoExchange with missing additive metadata response columns" begin
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        response_table =
            native_julia_doexchange_table(port; metadata = metadata_request_metadata())
        response = Tables.columntable(response_table)
        assert_metadata_scoring_columns(
            response,
            Union{Missing,String}[missing, missing],
            Union{Missing,String}[missing, missing],
            Union{Missing,Int64}[missing, missing],
            Union{Missing,WendaoArrow.CacheBackend}[missing, missing],
            Union{Missing,WendaoArrow.CacheScope}[missing, missing],
            Union{Missing,WendaoArrow.RankingStrategy}[missing, missing],
            Union{Missing,WendaoArrow.LinkGraphRetrievalMode}[missing, missing],
            Union{Missing,Bool}[missing, missing],
            Union{Missing,Float64}[missing, missing],
            Union{Missing,DateTime}[missing, missing],
        )
        @test Arrow.getmetadata(Tables.getcolumn(response_table, :trace_id))["semantic.role"] ==
              "trace-id"
        @test Arrow.getmetadata(Tables.getcolumn(response_table, :attempt_count))["semantic.role"] ==
              "attempt-count"
        @test Arrow.getmetadata(Tables.getcolumn(response_table, :cache_backend))["semantic.role"] ==
              "cache-backend"
        @test Arrow.getmetadata(Tables.getcolumn(response_table, :ranking_strategy))["semantic.role"] ==
              "ranking-strategy"
    end
end

@testset "cross-process schema-metadata Flight startup script preserves response schema metadata for pyarrow" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_schema_metadata_flight_server() do port, process
        @test Base.process_running(process)
        output =
            read_pyarrow_output(pyarrow_schema_metadata_doexchange_command(python, port))
        @test chomp(output) ==
              "{\"analyzer_score\": [0.9, 0.5], \"analyzer_score_field_metadata\": {\"semantic.role\": \"analyzer-score\"}, \"doc_id\": [\"doc-a\", \"doc-b\"], \"final_score\": [0.9, 0.5], \"final_score_field_metadata\": {\"semantic.role\": \"final-score\"}, \"schema_metadata\": {\"analyzer.name\": \"flight-schema-metadata-demo\", \"response.mode\": \"passthrough\", \"wendao.schema_version\": \"v1\"}}"
    end
end

@testset "cross-process schema-metadata Flight startup script preserves response schema metadata for native Julia" begin
    with_schema_metadata_flight_server() do port, process
        @test Base.process_running(process)
        response_table = native_julia_doexchange_table(port)
        response = Tables.columntable(response_table)
        metadata = WendaoArrow.schema_metadata(response_table)

        @test collect(response.doc_id) == ["doc-a", "doc-b"]
        @test collect(response.analyzer_score) == [0.9, 0.5]
        @test collect(response.final_score) == [0.9, 0.5]
        @test metadata["wendao.schema_version"] == WendaoArrow.DEFAULT_SCHEMA_VERSION
        @test metadata["analyzer.name"] == "flight-schema-metadata-demo"
        @test metadata["response.mode"] == "passthrough"
        @test Arrow.getmetadata(Tables.getcolumn(response_table, :analyzer_score))["semantic.role"] ==
              "analyzer-score"
        @test Arrow.getmetadata(Tables.getcolumn(response_table, :final_score))["semantic.role"] ==
              "final-score"
    end
end

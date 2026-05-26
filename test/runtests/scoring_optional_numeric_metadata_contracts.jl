@testset "Scoring response helper rejects optional numeric and enum metadata" begin
    invalid_cache_score_err = try
        WendaoArrow.normalize_scoring_response(
            (
                doc_id = ["doc-a"],
                analyzer_score = [1.0],
                final_score = [1.5],
                trace_id = ["trace-a"],
                tenant_id = ["tenant-a"],
                attempt_count = ["3"],
                cache_hit = ["true"],
                cache_score = ["oops"],
                cache_generated_at = ["2026-03-30T09:00:00"],
                cache_backend = ["memory"],
            );
            subject = "stream metadata response",
            optional_string_columns = ("trace_id", "tenant_id"),
            optional_int64_columns = ("attempt_count",),
            optional_bool_columns = ("cache_hit",),
            optional_float64_columns = ("cache_score",),
            optional_datetime_columns = ("cache_generated_at",),
            optional_enum_string_columns = (
                "cache_backend" => ("memory", "disk", "remote"),
            ),
        )
        nothing
    catch error
        error
    end
    @test invalid_cache_score_err isa ArgumentError
    @test occursin(
        "stream metadata response column cache_score row 1 must contain finite Float64 values or missing",
        sprint(showerror, invalid_cache_score_err),
    )
    @test occursin("\"oops\"::String", sprint(showerror, invalid_cache_score_err))

    invalid_cache_generated_at_err = try
        WendaoArrow.normalize_scoring_response(
            (
                doc_id = ["doc-a"],
                analyzer_score = [1.0],
                final_score = [1.5],
                trace_id = ["trace-a"],
                tenant_id = ["tenant-a"],
                attempt_count = ["3"],
                cache_hit = ["true"],
                cache_score = ["0.75"],
                cache_generated_at = ["not-a-datetime"],
                cache_backend = ["memory"],
                cache_scope = ["request"],
            );
            subject = "stream metadata response",
            optional_string_columns = ("trace_id", "tenant_id"),
            optional_int64_columns = ("attempt_count",),
            optional_bool_columns = ("cache_hit",),
            optional_float64_columns = ("cache_score",),
            optional_datetime_columns = ("cache_generated_at",),
            optional_enum_string_columns = (
                "cache_backend" => ("memory", "disk", "remote"),
                "cache_scope" => ("request", "tenant", "global"),
            ),
        )
        nothing
    catch error
        error
    end
    @test invalid_cache_generated_at_err isa ArgumentError
    @test occursin(
        "stream metadata response column cache_generated_at row 1 must contain ISO8601 DateTime values or missing",
        sprint(showerror, invalid_cache_generated_at_err),
    )
    @test occursin(
        "\"not-a-datetime\"::String",
        sprint(showerror, invalid_cache_generated_at_err),
    )

    invalid_cache_backend_err = try
        WendaoArrow.normalize_scoring_response(
            (
                doc_id = ["doc-a"],
                analyzer_score = [1.0],
                final_score = [1.5],
                trace_id = ["trace-a"],
                tenant_id = ["tenant-a"],
                attempt_count = ["3"],
                cache_hit = ["true"],
                cache_score = ["0.75"],
                cache_generated_at = ["2026-03-30T09:00:00"],
                cache_backend = ["sideways"],
                cache_scope = ["request"],
            );
            subject = "stream metadata response",
            optional_string_columns = ("trace_id", "tenant_id"),
            optional_int64_columns = ("attempt_count",),
            optional_bool_columns = ("cache_hit",),
            optional_float64_columns = ("cache_score",),
            optional_datetime_columns = ("cache_generated_at",),
            optional_enum_string_columns = (
                "cache_backend" => ("memory", "disk", "remote"),
                "cache_scope" => ("request", "tenant", "global"),
            ),
        )
        nothing
    catch error
        error
    end
    @test invalid_cache_backend_err isa ArgumentError
    @test occursin(
        "stream metadata response column cache_backend row 1 must contain one of [memory, disk, remote] or missing",
        sprint(showerror, invalid_cache_backend_err),
    )
    @test occursin("\"sideways\"::String", sprint(showerror, invalid_cache_backend_err))

    invalid_cache_scope_err = try
        WendaoArrow.normalize_scoring_response(
            (
                doc_id = ["doc-a"],
                analyzer_score = [1.0],
                final_score = [1.5],
                trace_id = ["trace-a"],
                tenant_id = ["tenant-a"],
                attempt_count = ["3"],
                cache_hit = ["true"],
                cache_score = ["0.75"],
                cache_generated_at = ["2026-03-30T09:00:00"],
                cache_backend = ["memory"],
                cache_scope = ["cluster"],
            );
            subject = "stream metadata response",
            optional_string_columns = ("trace_id", "tenant_id"),
            optional_int64_columns = ("attempt_count",),
            optional_bool_columns = ("cache_hit",),
            optional_float64_columns = ("cache_score",),
            optional_datetime_columns = ("cache_generated_at",),
            optional_enum_string_columns = (
                "cache_backend" => ("memory", "disk", "remote"),
                "cache_scope" => ("request", "tenant", "global"),
            ),
        )
        nothing
    catch error
        error
    end
    @test invalid_cache_scope_err isa ArgumentError
    @test occursin(
        "stream metadata response column cache_scope row 1 must contain one of [request, tenant, global] or missing",
        sprint(showerror, invalid_cache_scope_err),
    )
    @test occursin("\"cluster\"::String", sprint(showerror, invalid_cache_scope_err))

    invalid_retrieval_mode_err = try
        WendaoArrow.normalize_scoring_response(
            (
                doc_id = ["doc-a"],
                analyzer_score = [1.0],
                final_score = [1.5],
                trace_id = ["trace-a"],
                tenant_id = ["tenant-a"],
                attempt_count = ["3"],
                cache_hit = ["true"],
                cache_score = ["0.75"],
                cache_generated_at = ["2026-03-30T09:00:00"],
                cache_backend = [WendaoArrow.memory],
                cache_scope = [WendaoArrow.request],
                retrieval_mode = ["semantic_only"],
            );
            subject = "stream metadata response",
            optional_string_columns = ("trace_id", "tenant_id"),
            optional_int64_columns = ("attempt_count",),
            optional_bool_columns = ("cache_hit",),
            optional_float64_columns = ("cache_score",),
            optional_datetime_columns = ("cache_generated_at",),
            optional_enum_columns = (
                "cache_backend" => WendaoArrow.CacheBackend,
                "cache_scope" => WendaoArrow.CacheScope,
                "retrieval_mode" => WendaoArrow.LinkGraphRetrievalMode,
            ),
        )
        nothing
    catch error
        error
    end
    @test invalid_retrieval_mode_err isa ArgumentError
    @test occursin(
        "stream metadata response column retrieval_mode row 1 must contain one of [graph_only, hybrid, vector_only] or missing",
        sprint(showerror, invalid_retrieval_mode_err),
    )
    @test occursin(
        "\"semantic_only\"::String",
        sprint(showerror, invalid_retrieval_mode_err),
    )

    invalid_ranking_strategy_err = try
        WendaoArrow.normalize_scoring_response(
            (
                doc_id = ["doc-a"],
                analyzer_score = [1.0],
                final_score = [1.5],
                trace_id = ["trace-a"],
                tenant_id = ["tenant-a"],
                attempt_count = ["3"],
                cache_hit = ["true"],
                cache_score = ["0.75"],
                cache_generated_at = ["2026-03-30T09:00:00"],
                cache_backend = ["memory"],
                cache_scope = ["request"],
                ranking_strategy = ["graph-only"],
            );
            subject = "stream metadata response",
            optional_string_columns = ("trace_id", "tenant_id"),
            optional_int64_columns = ("attempt_count",),
            optional_bool_columns = ("cache_hit",),
            optional_float64_columns = ("cache_score",),
            optional_datetime_columns = ("cache_generated_at",),
            optional_enum_columns = (
                "cache_backend" => WendaoArrow.CacheBackend,
                "cache_scope" => WendaoArrow.CacheScope,
                "ranking_strategy" => WendaoArrow.RankingStrategy,
            ),
        )
        nothing
    catch error
        error
    end
    @test invalid_ranking_strategy_err isa ArgumentError
    @test occursin(
        "stream metadata response column ranking_strategy row 1 must contain one of [lexical, semantic, hybrid] or missing",
        sprint(showerror, invalid_ranking_strategy_err),
    )
    @test occursin(
        "\"graph-only\"::String",
        sprint(showerror, invalid_ranking_strategy_err),
    )
end

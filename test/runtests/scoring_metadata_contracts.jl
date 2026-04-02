@testset "Scoring response helper normalizes contract columns" begin
    normalized = WendaoArrow.normalize_scoring_response(
        (
            doc_id = ["doc-a", SubString("doc-b", 1, 5)],
            analyzer_score = [1, 0.5],
            final_score = [1.5f0, 0.75],
            trace_id = ["trace-a", "trace-b"],
        );
        subject = "stream scoring response",
    )
    @test normalized.doc_id == ["doc-a", "doc-b"]
    @test normalized.analyzer_score == [1.0, 0.5]
    @test normalized.final_score == [1.5, 0.75]
    @test normalized.trace_id == ["trace-a", "trace-b"]

    missing_column_err = try
        WendaoArrow.normalize_scoring_response(
            (doc_id = ["doc-a"], analyzer_score = [1.0]);
            subject = "stream scoring response",
        )
        nothing
    catch error
        error
    end
    @test missing_column_err isa ArgumentError
    @test occursin(
        "stream scoring response requires columns: doc_id, analyzer_score, final_score",
        sprint(showerror, missing_column_err),
    )

    finite_err = try
        WendaoArrow.normalize_scoring_response(
            (doc_id = ["doc-a"], analyzer_score = [NaN], final_score = [1.0]);
            subject = "stream scoring response",
        )
        nothing
    catch error
        error
    end
    @test finite_err isa ArgumentError
    @test occursin(
        "stream scoring response column analyzer_score row 1 must contain finite numeric values",
        sprint(showerror, finite_err),
    )
    @test occursin("got NaN::Float64", sprint(showerror, finite_err))
end

@testset "Scoring response helper preserves schema and column metadata" begin
    source = arrow_table_with_metadata(
        (
            doc_id = ["doc-a", "doc-b"],
            analyzer_score = [1, 0.5],
            final_score = [1.5f0, 0.75],
        );
        metadata = [
            "wendao.schema_version" => "shadowed",
            "analyzer.name" => "normalized-response-demo",
        ],
        colmetadata = Dict(
            :analyzer_score => ["semantic.role" => "analyzer-score"],
            :final_score => ["semantic.role" => "final-score"],
        ),
    )
    normalized =
        WendaoArrow.normalize_scoring_response(source; subject = "stream scoring response")

    @test normalized.doc_id == ["doc-a", "doc-b"]
    @test normalized.analyzer_score == [1.0, 0.5]
    @test normalized.final_score == [1.5, 0.75]
    @test WendaoArrow.schema_metadata(normalized)["wendao.schema_version"] == "shadowed"
    @test WendaoArrow.schema_metadata(normalized)["analyzer.name"] ==
          "normalized-response-demo"
    @test column_metadata(normalized, :analyzer_score)["semantic.role"] == "analyzer-score"
    @test column_metadata(normalized, :final_score)["semantic.role"] == "final-score"
end

@testset "Scoring response helper validates multiple optional typed columns" begin
    normalized = WendaoArrow.normalize_scoring_response(
        (
            doc_id = ["doc-a", "doc-b"],
            analyzer_score = [1.0, 0.5],
            final_score = [1.5, 0.75],
            trace_id = [missing, SubString("trace-b", 1, 7)],
            tenant_id = ["tenant-a", missing],
            attempt_count = [missing, "3"],
            cache_hit = [missing, "true"],
            cache_score = [missing, "0.75"],
            cache_generated_at = [missing, "2026-03-30T09:00:00"],
            cache_backend = [missing, "remote"],
            cache_scope = [missing, "tenant"],
            ranking_strategy = [missing, "hybrid"],
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
    @test isequal(normalized.trace_id, Union{Missing,String}[missing, "trace-b"])
    @test isequal(normalized.tenant_id, Union{Missing,String}["tenant-a", missing])
    @test isequal(normalized.attempt_count, Union{Missing,Int64}[missing, 3])
    @test isequal(normalized.cache_hit, Union{Missing,Bool}[missing, true])
    @test isequal(normalized.cache_score, Union{Missing,Float64}[missing, 0.75])
    @test isequal(
        normalized.cache_generated_at,
        Union{Missing,DateTime}[missing, DateTime(2026, 3, 30, 9, 0, 0)],
    )
    @test isequal(
        normalized.cache_backend,
        Union{Missing,WendaoArrow.CacheBackend}[missing, WendaoArrow.remote],
    )
    @test isequal(
        normalized.cache_scope,
        Union{Missing,WendaoArrow.CacheScope}[missing, WendaoArrow.tenant],
    )
    @test isequal(
        normalized.ranking_strategy,
        Union{Missing,WendaoArrow.RankingStrategy}[missing, WendaoArrow.hybrid],
    )

    invalid_trace_id_err = try
        WendaoArrow.normalize_scoring_response(
            (
                doc_id = ["doc-a"],
                analyzer_score = [1.0],
                final_score = [1.5],
                trace_id = [42],
                tenant_id = ["tenant-a"],
                attempt_count = ["3"],
                cache_hit = ["true"],
                cache_score = ["0.75"],
                cache_generated_at = ["2026-03-30T09:00:00"],
                cache_backend = ["memory"],
                cache_scope = ["request"],
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
            ),
        )
        nothing
    catch error
        error
    end
    @test invalid_trace_id_err isa ArgumentError
    @test occursin(
        "stream metadata response column trace_id row 1 must contain string values or missing",
        sprint(showerror, invalid_trace_id_err),
    )
    @test occursin("42::Int64", sprint(showerror, invalid_trace_id_err))

    empty_trace_id_err = try
        WendaoArrow.normalize_scoring_response(
            (
                doc_id = ["doc-a"],
                analyzer_score = [1.0],
                final_score = [1.5],
                trace_id = [""],
                tenant_id = ["tenant-a"],
                attempt_count = ["3"],
                cache_hit = ["true"],
                cache_score = ["0.75"],
                cache_generated_at = ["2026-03-30T09:00:00"],
                cache_backend = ["memory"],
                cache_scope = ["request"],
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
            ),
        )
        nothing
    catch error
        error
    end
    @test empty_trace_id_err isa ArgumentError
    @test occursin(
        "stream metadata response column trace_id row 1 must contain non-empty string values or missing",
        sprint(showerror, empty_trace_id_err),
    )

    mismatched_trace_id_err = try
        WendaoArrow.normalize_scoring_response(
            (
                doc_id = ["doc-a", "doc-b"],
                analyzer_score = [1.0, 0.5],
                final_score = [1.5, 0.75],
                trace_id = ["trace-a"],
                tenant_id = ["tenant-a", "tenant-b"],
                attempt_count = ["3", "3"],
                cache_hit = ["true", "false"],
                cache_score = ["0.75", "0.25"],
                cache_generated_at = ["2026-03-30T09:00:00", "2026-03-30T09:05:00"],
                cache_backend = ["memory", "remote"],
                cache_scope = ["request", "tenant"],
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
    @test mismatched_trace_id_err isa ArgumentError
    @test occursin(
        "stream metadata response requires aligned column lengths",
        sprint(showerror, mismatched_trace_id_err),
    )
    @test occursin("trace_id=1", sprint(showerror, mismatched_trace_id_err))

    invalid_tenant_id_err = try
        WendaoArrow.normalize_scoring_response(
            (
                doc_id = ["doc-a"],
                analyzer_score = [1.0],
                final_score = [1.5],
                trace_id = ["trace-a"],
                tenant_id = [""],
                attempt_count = ["3"],
                cache_hit = ["true"],
                cache_score = ["0.75"],
                cache_generated_at = ["2026-03-30T09:00:00"],
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
    @test invalid_tenant_id_err isa ArgumentError
    @test occursin(
        "stream metadata response column tenant_id row 1 must contain non-empty string values or missing",
        sprint(showerror, invalid_tenant_id_err),
    )

    invalid_attempt_count_err = try
        WendaoArrow.normalize_scoring_response(
            (
                doc_id = ["doc-a"],
                analyzer_score = [1.0],
                final_score = [1.5],
                trace_id = ["trace-a"],
                tenant_id = ["tenant-a"],
                attempt_count = ["oops"],
                cache_hit = ["true"],
                cache_score = ["0.75"],
                cache_generated_at = ["2026-03-30T09:00:00"],
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
    @test invalid_attempt_count_err isa ArgumentError
    @test occursin(
        "stream metadata response column attempt_count row 1 must contain Int64 values or missing",
        sprint(showerror, invalid_attempt_count_err),
    )
    @test occursin("\"oops\"::String", sprint(showerror, invalid_attempt_count_err))

    invalid_cache_hit_err = try
        WendaoArrow.normalize_scoring_response(
            (
                doc_id = ["doc-a"],
                analyzer_score = [1.0],
                final_score = [1.5],
                trace_id = ["trace-a"],
                tenant_id = ["tenant-a"],
                attempt_count = ["3"],
                cache_hit = ["maybe"],
                cache_score = ["0.75"],
                cache_generated_at = ["2026-03-30T09:00:00"],
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
    @test invalid_cache_hit_err isa ArgumentError
    @test occursin(
        "stream metadata response column cache_hit row 1 must contain Bool values or missing",
        sprint(showerror, invalid_cache_hit_err),
    )
    @test occursin("\"maybe\"::String", sprint(showerror, invalid_cache_hit_err))

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

@testset "Metadata normalization helpers validate additive metadata keys" begin
    metadata = Dict(
        "trace_id" => "trace-123",
        "tenant_id" => "tenant-7",
        "attempt_count" => "3",
        "cache_hit" => "true",
        "cache_score" => "0.75",
        "cache_generated_at" => "2026-03-30T09:00:00",
        "cache_backend" => "remote",
        "cache_scope" => "tenant",
        "ranking_strategy" => "hybrid",
        "retrieval_mode" => "hybrid",
    )

    normalized_metadata = WendaoArrow.normalize_metadata_values(
        metadata;
        string_keys = ("trace_id", "tenant_id"),
        int64_keys = ("attempt_count",),
        bool_keys = ("cache_hit",),
        float64_keys = ("cache_score",),
        datetime_keys = ("cache_generated_at",),
        enum_keys = (
            "cache_backend" => WendaoArrow.CacheBackend,
            "cache_scope" => WendaoArrow.CacheScope,
            "ranking_strategy" => WendaoArrow.RankingStrategy,
            "retrieval_mode" => WendaoArrow.LinkGraphRetrievalMode,
        ),
        subject = "stream metadata request metadata",
    )
    @test normalized_metadata["trace_id"] == "trace-123"
    @test normalized_metadata["tenant_id"] == "tenant-7"
    @test normalized_metadata["attempt_count"] == 3
    @test normalized_metadata["cache_hit"] === true
    @test normalized_metadata["cache_score"] == 0.75
    @test normalized_metadata["cache_generated_at"] == DateTime(2026, 3, 30, 9, 0, 0)
    @test normalized_metadata["cache_backend"] == WendaoArrow.remote
    @test normalized_metadata["cache_scope"] == WendaoArrow.tenant
    @test normalized_metadata["ranking_strategy"] == WendaoArrow.hybrid
    @test normalized_metadata["retrieval_mode"] ==
          WendaoArrow.LinkGraphRetrievalModes.hybrid

    @test WendaoArrow.coerce_metadata_optional_string(
        metadata,
        "trace_id";
        subject = "stream metadata request metadata",
    ) == "trace-123"
    @test WendaoArrow.coerce_metadata_optional_string(
        metadata,
        "tenant_id";
        subject = "stream metadata request metadata",
    ) == "tenant-7"
    @test WendaoArrow.coerce_metadata_optional_int64(
        metadata,
        "attempt_count";
        subject = "stream metadata request metadata",
    ) == 3
    @test WendaoArrow.coerce_metadata_optional_bool(
        metadata,
        "cache_hit";
        subject = "stream metadata request metadata",
    ) === true
    @test WendaoArrow.coerce_metadata_optional_float64(
        metadata,
        "cache_score";
        subject = "stream metadata request metadata",
    ) == 0.75
    @test WendaoArrow.coerce_metadata_optional_datetime(
        metadata,
        "cache_generated_at";
        subject = "stream metadata request metadata",
    ) == DateTime(2026, 3, 30, 9, 0, 0)
    @test WendaoArrow.coerce_metadata_optional_enum(
        metadata,
        "cache_backend";
        subject = "stream metadata request metadata",
        enum_type = WendaoArrow.CacheBackend,
    ) == WendaoArrow.remote
    @test WendaoArrow.coerce_metadata_optional_enum(
        metadata,
        "cache_scope";
        subject = "stream metadata request metadata",
        enum_type = WendaoArrow.CacheScope,
    ) == WendaoArrow.tenant
    @test WendaoArrow.coerce_metadata_optional_enum(
        metadata,
        "ranking_strategy";
        subject = "stream metadata request metadata",
        enum_type = WendaoArrow.RankingStrategy,
    ) == WendaoArrow.hybrid
    @test WendaoArrow.coerce_metadata_optional_enum(
        metadata,
        "retrieval_mode";
        subject = "stream metadata request metadata",
        enum_type = WendaoArrow.LinkGraphRetrievalMode,
    ) == WendaoArrow.LinkGraphRetrievalModes.hybrid
    @test ismissing(
        WendaoArrow.coerce_metadata_optional_string(
            metadata,
            "missing_key";
            subject = "stream metadata request metadata",
        ),
    )
    @test ismissing(
        WendaoArrow.coerce_metadata_optional_int64(
            metadata,
            "missing_count";
            subject = "stream metadata request metadata",
        ),
    )
    @test ismissing(
        WendaoArrow.coerce_metadata_optional_bool(
            metadata,
            "missing_bool";
            subject = "stream metadata request metadata",
        ),
    )
    @test ismissing(
        WendaoArrow.coerce_metadata_optional_float64(
            metadata,
            "missing_float";
            subject = "stream metadata request metadata",
        ),
    )
    @test ismissing(
        WendaoArrow.coerce_metadata_optional_datetime(
            metadata,
            "missing_datetime";
            subject = "stream metadata request metadata",
        ),
    )
    @test ismissing(
        WendaoArrow.coerce_metadata_optional_enum(
            metadata,
            "missing_enum";
            subject = "stream metadata request metadata",
            enum_type = WendaoArrow.CacheBackend,
        ),
    )
    @test ismissing(
        WendaoArrow.coerce_metadata_optional_enum(
            metadata,
            "missing_scope";
            subject = "stream metadata request metadata",
            enum_type = WendaoArrow.CacheScope,
        ),
    )
    @test ismissing(
        WendaoArrow.coerce_metadata_optional_enum(
            metadata,
            "missing_strategy";
            subject = "stream metadata request metadata",
            enum_type = WendaoArrow.RankingStrategy,
        ),
    )
    @test ismissing(
        WendaoArrow.coerce_metadata_optional_enum(
            metadata,
            "missing_retrieval_mode";
            subject = "stream metadata request metadata",
            enum_type = WendaoArrow.LinkGraphRetrievalMode,
        ),
    )

    invalid_trace_id_err = try
        WendaoArrow.coerce_metadata_optional_string(
            Dict("trace_id" => ""),
            "trace_id";
            subject = "stream metadata request metadata",
        )
        nothing
    catch error
        error
    end
    @test invalid_trace_id_err isa ArgumentError
    @test occursin(
        "stream metadata request metadata key trace_id must contain non-empty string values or be missing",
        sprint(showerror, invalid_trace_id_err),
    )

    invalid_tenant_id_err = try
        WendaoArrow.coerce_metadata_optional_string(
            Dict("tenant_id" => ""),
            "tenant_id";
            subject = "stream metadata request metadata",
        )
        nothing
    catch error
        error
    end
    @test invalid_tenant_id_err isa ArgumentError
    @test occursin(
        "stream metadata request metadata key tenant_id must contain non-empty string values or be missing",
        sprint(showerror, invalid_tenant_id_err),
    )

    invalid_attempt_count_err = try
        WendaoArrow.coerce_metadata_optional_int64(
            Dict("attempt_count" => "oops"),
            "attempt_count";
            subject = "stream metadata request metadata",
        )
        nothing
    catch error
        error
    end
    @test invalid_attempt_count_err isa ArgumentError
    @test occursin(
        "stream metadata request metadata key attempt_count must contain Int64 values or be missing",
        sprint(showerror, invalid_attempt_count_err),
    )
    @test occursin("\"oops\"::String", sprint(showerror, invalid_attempt_count_err))

    invalid_cache_hit_err = try
        WendaoArrow.coerce_metadata_optional_bool(
            Dict("cache_hit" => "maybe"),
            "cache_hit";
            subject = "stream metadata request metadata",
        )
        nothing
    catch error
        error
    end
    @test invalid_cache_hit_err isa ArgumentError
    @test occursin(
        "stream metadata request metadata key cache_hit must contain Bool values or be missing",
        sprint(showerror, invalid_cache_hit_err),
    )
    @test occursin("\"maybe\"::String", sprint(showerror, invalid_cache_hit_err))

    invalid_cache_score_err = try
        WendaoArrow.coerce_metadata_optional_float64(
            Dict("cache_score" => "oops"),
            "cache_score";
            subject = "stream metadata request metadata",
        )
        nothing
    catch error
        error
    end
    @test invalid_cache_score_err isa ArgumentError
    @test occursin(
        "stream metadata request metadata key cache_score must contain finite Float64 values or be missing",
        sprint(showerror, invalid_cache_score_err),
    )
    @test occursin("\"oops\"::String", sprint(showerror, invalid_cache_score_err))

    invalid_cache_generated_at_err = try
        WendaoArrow.coerce_metadata_optional_datetime(
            Dict("cache_generated_at" => "not-a-datetime"),
            "cache_generated_at";
            subject = "stream metadata request metadata",
        )
        nothing
    catch error
        error
    end
    @test invalid_cache_generated_at_err isa ArgumentError
    @test occursin(
        "stream metadata request metadata key cache_generated_at must contain ISO8601 DateTime values or be missing",
        sprint(showerror, invalid_cache_generated_at_err),
    )
    @test occursin(
        "\"not-a-datetime\"::String",
        sprint(showerror, invalid_cache_generated_at_err),
    )

    invalid_cache_backend_err = try
        WendaoArrow.coerce_metadata_optional_enum(
            Dict("cache_backend" => "sideways"),
            "cache_backend";
            subject = "stream metadata request metadata",
            enum_type = WendaoArrow.CacheBackend,
        )
        nothing
    catch error
        error
    end
    @test invalid_cache_backend_err isa ArgumentError
    @test occursin(
        "stream metadata request metadata key cache_backend must contain one of [memory, disk, remote] or be missing",
        sprint(showerror, invalid_cache_backend_err),
    )
    @test occursin("\"sideways\"::String", sprint(showerror, invalid_cache_backend_err))

    invalid_cache_scope_err = try
        WendaoArrow.coerce_metadata_optional_enum(
            Dict("cache_scope" => "cluster"),
            "cache_scope";
            subject = "stream metadata request metadata",
            enum_type = WendaoArrow.CacheScope,
        )
        nothing
    catch error
        error
    end
    @test invalid_cache_scope_err isa ArgumentError
    @test occursin(
        "stream metadata request metadata key cache_scope must contain one of [request, tenant, global] or be missing",
        sprint(showerror, invalid_cache_scope_err),
    )
    @test occursin("\"cluster\"::String", sprint(showerror, invalid_cache_scope_err))

    invalid_ranking_strategy_err = try
        WendaoArrow.coerce_metadata_optional_enum(
            Dict("ranking_strategy" => "graph-only"),
            "ranking_strategy";
            subject = "stream metadata request metadata",
            enum_type = WendaoArrow.RankingStrategy,
        )
        nothing
    catch error
        error
    end
    @test invalid_ranking_strategy_err isa ArgumentError
    @test occursin(
        "stream metadata request metadata key ranking_strategy must contain one of [lexical, semantic, hybrid] or be missing",
        sprint(showerror, invalid_ranking_strategy_err),
    )
    @test occursin(
        "\"graph-only\"::String",
        sprint(showerror, invalid_ranking_strategy_err),
    )

    invalid_retrieval_mode_err = try
        WendaoArrow.coerce_metadata_optional_enum(
            Dict("retrieval_mode" => "semantic_only"),
            "retrieval_mode";
            subject = "stream metadata request metadata",
            enum_type = WendaoArrow.LinkGraphRetrievalMode,
        )
        nothing
    catch error
        error
    end
    @test invalid_retrieval_mode_err isa ArgumentError
    @test occursin(
        "stream metadata request metadata key retrieval_mode must contain one of [graph_only, hybrid, vector_only] or be missing",
        sprint(showerror, invalid_retrieval_mode_err),
    )
    @test occursin(
        "\"semantic_only\"::String",
        sprint(showerror, invalid_retrieval_mode_err),
    )

    duplicate_key_err = try
        WendaoArrow.normalize_metadata_values(
            metadata;
            string_keys = ("trace_id",),
            enum_keys = ("trace_id" => WendaoArrow.RankingStrategy,),
            subject = "stream metadata request metadata",
        )
        nothing
    catch error
        error
    end
    @test duplicate_key_err isa ArgumentError
    @test occursin(
        "stream metadata request metadata declares duplicate metadata keys across type groups: trace_id",
        sprint(showerror, duplicate_key_err),
    )
end

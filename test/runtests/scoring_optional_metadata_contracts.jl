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

end

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

const STREAM_METADATA_STRING_KEYS = ("trace_id", "tenant_id")
const STREAM_METADATA_INT64_KEYS = ("attempt_count",)
const STREAM_METADATA_BOOL_KEYS = ("cache_hit",)
const STREAM_METADATA_FLOAT64_KEYS = ("cache_score",)
const STREAM_METADATA_DATETIME_KEYS = ("cache_generated_at",)
const STREAM_METADATA_ENUM_KEYS = (
    "cache_backend" => WendaoArrow.CacheBackend,
    "cache_scope" => WendaoArrow.CacheScope,
    "ranking_strategy" => WendaoArrow.RankingStrategy,
    "retrieval_mode" => WendaoArrow.LinkGraphRetrievalMode,
)
const STREAM_METADATA_RESPONSE_COLMETADATA = Dict(
    :trace_id => ["semantic.role" => "trace-id"],
    :tenant_id => ["semantic.role" => "tenant-id"],
    :attempt_count => ["semantic.role" => "attempt-count"],
    :cache_hit => ["semantic.role" => "cache-hit"],
    :cache_score => ["semantic.role" => "cache-score"],
    :cache_generated_at => ["semantic.role" => "cache-generated-at"],
    :cache_backend => ["semantic.role" => "cache-backend"],
    :cache_scope => ["semantic.role" => "cache-scope"],
    :ranking_strategy => ["semantic.role" => "ranking-strategy"],
    :retrieval_mode => ["semantic.role" => "retrieval-mode"],
)

function normalize_stream_metadata_request(metadata; subject)
    return WendaoArrow.normalize_metadata_values(
        metadata;
        string_keys = STREAM_METADATA_STRING_KEYS,
        int64_keys = STREAM_METADATA_INT64_KEYS,
        bool_keys = STREAM_METADATA_BOOL_KEYS,
        float64_keys = STREAM_METADATA_FLOAT64_KEYS,
        datetime_keys = STREAM_METADATA_DATETIME_KEYS,
        enum_keys = STREAM_METADATA_ENUM_KEYS,
        subject = subject,
    )
end

function normalize_stream_metadata_response(response; subject)
    return WendaoArrow.normalize_scoring_response(
        response;
        subject = subject,
        optional_string_columns = STREAM_METADATA_STRING_KEYS,
        optional_int64_columns = STREAM_METADATA_INT64_KEYS,
        optional_bool_columns = STREAM_METADATA_BOOL_KEYS,
        optional_float64_columns = STREAM_METADATA_FLOAT64_KEYS,
        optional_datetime_columns = STREAM_METADATA_DATETIME_KEYS,
        optional_enum_columns = STREAM_METADATA_ENUM_KEYS,
    )
end

function attach_stream_metadata_response_metadata(response)
    return Arrow.withmetadata(response; colmetadata = STREAM_METADATA_RESPONSE_COLMETADATA)
end

function normalize_stream_request_rows(
    batch;
    subject,
    seen_doc_ids::AbstractDict{String,Int},
    row_offset::Integer,
)
    WendaoArrow.require_columns(batch, ("doc_id", "vector_score"); subject = subject)
    row_count = WendaoArrow.require_column_lengths(
        batch,
        ("doc_id", "vector_score");
        subject = subject,
    )
    WendaoArrow.require_unique_string_column(
        batch,
        "doc_id";
        subject = subject,
        seen = seen_doc_ids,
        row_offset = row_offset,
    )

    columns = Tables.columntable(batch)
    normalized_rows = Vector{Tuple{String,Float64}}(undef, row_count)
    for (row_index, (doc_id, vector_score)) in
        enumerate(zip(columns.doc_id, columns.vector_score))
        normalized_rows[row_index] = (
            WendaoArrow.coerce_string(
                doc_id;
                column = "doc_id",
                subject = subject,
                row_index = row_index,
            ),
            WendaoArrow.coerce_float64(
                vector_score;
                column = "vector_score",
                subject = subject,
                row_index = row_index,
            ),
        )
    end
    return normalized_rows
end

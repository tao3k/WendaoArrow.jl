function init_stream_metadata_response_columns()
    return (
        doc_id = String[],
        analyzer_score = Float64[],
        final_score = Float64[],
        trace_id = Union{String,Missing}[],
        tenant_id = Union{String,Missing}[],
        attempt_count = Union{Int64,Missing}[],
        cache_hit = Union{Bool,Missing}[],
        cache_score = Union{Float64,Missing}[],
        cache_generated_at = Union{DateTime,Missing}[],
        cache_backend = Union{WendaoArrow.CacheBackend,Missing}[],
        cache_scope = Union{WendaoArrow.CacheScope,Missing}[],
        ranking_strategy = Union{WendaoArrow.RankingStrategy,Missing}[],
        retrieval_mode = Union{WendaoArrow.LinkGraphRetrievalMode,Missing}[],
    )
end

function append_stream_metadata_row!(
    response,
    normalized_metadata,
    doc_id::String,
    score::Float64,
)
    push!(response.doc_id, doc_id)
    push!(response.analyzer_score, score)
    push!(response.final_score, score)
    push!(response.trace_id, normalized_metadata["trace_id"])
    push!(response.tenant_id, normalized_metadata["tenant_id"])
    push!(response.attempt_count, normalized_metadata["attempt_count"])
    push!(response.cache_hit, normalized_metadata["cache_hit"])
    push!(response.cache_score, normalized_metadata["cache_score"])
    push!(response.cache_generated_at, normalized_metadata["cache_generated_at"])
    push!(response.cache_backend, normalized_metadata["cache_backend"])
    push!(response.cache_scope, normalized_metadata["cache_scope"])
    push!(response.ranking_strategy, normalized_metadata["ranking_strategy"])
    push!(response.retrieval_mode, normalized_metadata["retrieval_mode"])
    return nothing
end

function build_stream_metadata_example_processor(;
    request_batch_subject,
    request_metadata_subject,
    response_subject,
    response_mutator = identity,
)
    return function processor(stream)
        response = init_stream_metadata_response_columns()
        seen_doc_ids = Dict{String,Int}()
        row_offset = 0

        for batch in stream
            normalized_metadata = normalize_stream_metadata_request(
                WendaoArrow.schema_metadata(batch);
                subject = request_metadata_subject,
            )
            normalized_rows = normalize_stream_request_rows(
                batch;
                subject = request_batch_subject,
                seen_doc_ids = seen_doc_ids,
                row_offset = row_offset,
            )

            for (doc_id, score) in normalized_rows
                append_stream_metadata_row!(response, normalized_metadata, doc_id, score)
            end
            row_offset += length(normalized_rows)
        end

        return normalize_stream_metadata_response(
            attach_stream_metadata_response_metadata(response_mutator(response));
            subject = response_subject,
        )
    end
end

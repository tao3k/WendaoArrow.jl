
using WendaoArrow
using gRPCServer
using Tables

const SCORE_MAP = Dict(
    "alpha" => (0.2, 0.15),
    "beta" => (0.9, 0.97),
)

function processor(stream)
    doc_ids = String[]
    analyzer_scores = Float64[]
    final_scores = Float64[]
    seen_doc_ids = Dict{String,Int}()
    row_offset = 0

    for batch in stream
        WendaoArrow.require_columns(
            batch,
            ("doc_id", "vector_score");
            subject = "custom Julia rerank request",
        )
        row_count = WendaoArrow.require_column_lengths(
            batch,
            ("doc_id", "vector_score");
            subject = "custom Julia rerank request",
        )
        WendaoArrow.require_unique_string_column(
            batch,
            "doc_id";
            subject = "custom Julia rerank request",
            seen = seen_doc_ids,
            row_offset = row_offset,
        )

        columns = Tables.columntable(batch)
        sizehint!(doc_ids, length(doc_ids) + row_count)
        sizehint!(analyzer_scores, length(analyzer_scores) + row_count)
        sizehint!(final_scores, length(final_scores) + row_count)

        for (row_index, (raw_doc_id, raw_vector_score)) in
            enumerate(zip(columns.doc_id, columns.vector_score))
            doc_id = WendaoArrow.coerce_string(
                raw_doc_id;
                column = "doc_id",
                subject = "custom Julia rerank request",
                row_index = row_index,
            )
            WendaoArrow.coerce_float64(
                raw_vector_score;
                column = "vector_score",
                subject = "custom Julia rerank request",
                row_index = row_index,
            )
            analyzer_score, final_score = get(SCORE_MAP, doc_id, (0.0, 0.0))
            push!(doc_ids, doc_id)
            push!(analyzer_scores, analyzer_score)
            push!(final_scores, final_score)
        end

        row_offset += row_count
    end

    return WendaoArrow.normalize_scoring_response(
        (doc_id = doc_ids, analyzer_score = analyzer_scores, final_score = final_scores);
        subject = "custom Julia rerank response",
    )
end

config = WendaoArrow.config_from_args(ARGS)

WendaoArrow.serve_stream_flight(
    processor;
    descriptor = WendaoArrow.flight_descriptor(("rerank",)),
    host = config.host,
    port = config.port,
)

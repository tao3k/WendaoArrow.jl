
using WendaoArrow
using gRPCServer
using Tables

const SCORE_MAP = Dict(
    "alpha" => (0.2, 0.15),
    "beta" => (0.9, 0.97),
)

function processor(stream)
    table = Tables.columntable(stream)
    columns = Tables.columntable(table)
    analyzer_scores = Float64[]
    final_scores = Float64[]
    sizehint!(analyzer_scores, length(columns.doc_id))
    sizehint!(final_scores, length(columns.doc_id))

    for raw_doc_id in columns.doc_id
        doc_id = String(raw_doc_id)
        analyzer_score, final_score = get(SCORE_MAP, doc_id, (0.0, 0.0))
        push!(analyzer_scores, analyzer_score)
        push!(final_scores, final_score)
    end

    return (
        doc_id = collect(columns.doc_id),
        analyzer_score = analyzer_scores,
        final_score = final_scores,
    )
end

config = WendaoArrow.config_from_args(ARGS)

WendaoArrow.serve_stream_flight(
    processor;
    descriptor = WendaoArrow.flight_descriptor(("rerank",)),
    host = config.host,
    port = config.port,
)

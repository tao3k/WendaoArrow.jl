using WendaoArrow
using gRPCServer

include("support.jl")

using .WendaoArrowExampleSupport

processor = build_stream_scoring_example_processor(
    request_subject = "WendaoArrow stream scoring Flight request batch",
    response_subject = "WendaoArrow stream scoring Flight response",
    response_mutator = response -> merge(
        response,
        (
            analyzer_score = fill(NaN, length(response.doc_id)),
            final_score = fill(1.0, length(response.doc_id)),
        ),
    ),
)

config = WendaoArrow.config_from_args(ARGS)

WendaoArrow.serve_stream_flight(processor; host = config.host, port = config.port)

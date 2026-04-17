using WendaoArrow

include("support.jl")

using .WendaoArrowExampleSupport

processor = build_stream_scoring_example_processor(
    request_subject = "WendaoArrow stream scoring Flight request batch",
    response_subject = "WendaoArrow stream scoring Flight large response",
    response_mutator = _ -> (
        doc_id = [repeat("x", LARGE_RESPONSE_DOC_ID_BYTES)],
        analyzer_score = [0.9],
        final_score = [0.9],
    ),
)

config = WendaoArrow.config_from_args(ARGS)

WendaoArrow.serve_stream_flight(
    processor;
    descriptor = WendaoArrow.flight_descriptor(("rerank",)),
    host = config.host,
    port = config.port,
    max_message_size = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
)

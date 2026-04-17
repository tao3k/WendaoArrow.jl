using WendaoArrow

include("support.jl")

using .WendaoArrowExampleSupport

processor = build_parser_summary_request_example_processor(
    request_subject = "WendaoArrow parser-summary request Flight batch",
)

config = WendaoArrow.config_from_args(ARGS)

WendaoArrow.serve_stream_flight(
    processor;
    descriptor = WendaoArrow.flight_descriptor(("rerank",)),
    host = config.host,
    port = config.port,
    max_message_size = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
)

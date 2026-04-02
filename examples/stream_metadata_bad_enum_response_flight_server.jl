using WendaoArrow
using gRPCServer

include("support.jl")

using .WendaoArrowExampleSupport

processor = build_stream_metadata_example_processor(
    request_batch_subject = "WendaoArrow stream metadata Flight request batch",
    request_metadata_subject = "WendaoArrow stream metadata Flight request metadata",
    response_subject = "WendaoArrow stream metadata Flight response",
    response_mutator = response ->
        merge(response, (cache_backend = fill("sideways", length(response.doc_id)),)),
)

config = WendaoArrow.config_from_args(ARGS)

WendaoArrow.serve_stream_flight(processor; host = config.host, port = config.port)

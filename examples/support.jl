module WendaoArrowExampleSupport

using Dates
using Tables
using WendaoArrow

const Arrow = WendaoArrow.Arrow

export build_stream_metadata_example_processor
export build_parser_summary_like_example_processor
export build_parser_summary_request_example_processor
export build_list_roundtrip_example_processor
export build_stream_scoring_example_processor
export LARGE_RESPONSE_DOC_ID_BYTES
export PARSER_SUMMARY_LIKE_ROW_COUNT

include(joinpath("support", "common.jl"))
include(joinpath("support", "list_roundtrip.jl"))
include(joinpath("support", "scoring.jl"))
include(joinpath("support", "metadata.jl"))
include(joinpath("support", "parser_summary.jl"))

end

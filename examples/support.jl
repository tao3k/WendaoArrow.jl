module WendaoArrowExampleSupport

using Arrow
using Dates
using Tables
using WendaoArrow

export build_stream_metadata_example_processor
export build_list_roundtrip_example_processor
export build_stream_scoring_example_processor
export LARGE_RESPONSE_DOC_ID_BYTES

include(joinpath("support", "common.jl"))
include(joinpath("support", "list_roundtrip.jl"))
include(joinpath("support", "scoring.jl"))
include(joinpath("support", "metadata.jl"))

end

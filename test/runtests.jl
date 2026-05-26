using JuliaLangProjectHarness

run_harness_suite = isempty(ARGS) || "harness" in ARGS
run_unit_suite = isempty(ARGS) || "unit" in ARGS

if run_harness_suite
    JuliaLangProjectHarness.assert_julia_project_harness_test_profile_clean(dirname(@__DIR__))
end

if run_unit_suite
    include("runtests/support.jl")
    include("runtests/contract_helpers.jl")
    include("runtests/cdata.jl")
    include("runtests/schema_table_contracts.jl")
    include("runtests/scoring_metadata_contracts.jl")
    include("runtests/scoring_optional_metadata_contracts.jl")
    include("runtests/scoring_optional_numeric_metadata_contracts.jl")
    include("runtests/metadata_normalization_contracts.jl")
    include("runtests/local_flight.jl")
    include("runtests/local_flight_stream_failures.jl")
    include("runtests/gateway_flight.jl")
    include("runtests/packaged_flight_benchmark_server.jl")
    include("runtests/config_loading.jl")
end

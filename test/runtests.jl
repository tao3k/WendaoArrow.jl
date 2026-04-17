run_unit_suite = isempty(ARGS) || "unit" in ARGS

if run_unit_suite
    include("runtests/support.jl")
    include("runtests/contract_helpers.jl")
    include("runtests/schema_table_contracts.jl")
    include("runtests/scoring_metadata_contracts.jl")
    include("runtests/local_flight.jl")
    include("runtests/gateway_flight.jl")
    include("runtests/packaged_flight_benchmark_server.jl")
    include("runtests/config_loading.jl")
end

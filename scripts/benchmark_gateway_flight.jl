using Statistics
using Tables
using WendaoArrow

Base.@kwdef struct GatewayFlightBenchConfig
    host::String = WendaoArrow.DEFAULT_HOST
    port::Int = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_PORT
    query::String = "flight"
    limit::Int = 5
    samples::Int = 10
    route::Symbol = :both
end

function parse_bench_args(args::Vector{String})
    host = WendaoArrow.DEFAULT_HOST
    port = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_PORT
    query = "flight"
    limit = 5
    samples = 10
    route = :both
    index = 1
    while index <= length(args)
        argument = args[index]
        if startswith(argument, "--host=")
            host = split(argument, "=", limit = 2)[2]
        elseif startswith(argument, "--port=")
            port = parse(Int, split(argument, "=", limit = 2)[2])
        elseif startswith(argument, "--query=")
            query = split(argument, "=", limit = 2)[2]
        elseif startswith(argument, "--limit=")
            limit = parse(Int, split(argument, "=", limit = 2)[2])
        elseif startswith(argument, "--samples=")
            samples = parse(Int, split(argument, "=", limit = 2)[2])
        elseif startswith(argument, "--route=")
            route = Symbol(split(argument, "=", limit = 2)[2])
        elseif argument in
               ("--host", "--port", "--query", "--limit", "--samples", "--route")
            index += 1
            index > length(args) && throw(ArgumentError("missing value for $(argument)"))
            value = args[index]
            if argument == "--host"
                host = value
            elseif argument == "--port"
                port = parse(Int, value)
            elseif argument == "--query"
                query = value
            elseif argument == "--limit"
                limit = parse(Int, value)
            elseif argument == "--samples"
                samples = parse(Int, value)
            else
                route = Symbol(value)
            end
        else
            throw(ArgumentError("unsupported benchmark argument: $(argument)"))
        end
        index += 1
    end
    samples > 0 || throw(ArgumentError("benchmark samples must be greater than zero"))
    limit > 0 || throw(ArgumentError("benchmark limit must be greater than zero"))
    route in (:repo, :knowledge, :both) || throw(
        ArgumentError(
            "benchmark route must be one of :repo, :knowledge, or :both; got $(route)",
        ),
    )
    return GatewayFlightBenchConfig(;
        host = host,
        port = port,
        query = query,
        limit = limit,
        samples = samples,
        route = route,
    )
end

function bench_case(label::AbstractString, fetch::Function; samples::Int)
    warm_table = fetch()
    row_count = length(Tables.rows(warm_table))
    column_count = length(Tables.schema(warm_table).names)
    times_ms = Float64[]
    alloc_bytes = Int[]
    for _ = 1:samples
        GC.gc()
        result = @timed fetch()
        push!(times_ms, result.time * 1000)
        push!(alloc_bytes, result.bytes)
    end
    return (
        case = label,
        median_ms = median(times_ms),
        minimum_ms = minimum(times_ms),
        maximum_ms = maximum(times_ms),
        alloc_bytes = median(alloc_bytes),
        rows = row_count,
        columns = column_count,
    )
end

function print_results(results)
    println("case\tmedian_ms\tminimum_ms\tmaximum_ms\talloc_bytes\trows\tcolumns")
    for result in results
        println(
            join(
                (
                    result.case,
                    string(round(result.median_ms; digits = 3)),
                    string(round(result.minimum_ms; digits = 3)),
                    string(round(result.maximum_ms; digits = 3)),
                    string(result.alloc_bytes),
                    string(result.rows),
                    string(result.columns),
                ),
                '\t',
            ),
        )
    end
end

function main(args::Vector{String})
    config = parse_bench_args(args)
    client = WendaoArrow.gateway_flight_client(;
        host = config.host,
        port = config.port,
        deadline = 30,
    )
    results = NamedTuple[]
    if config.route in (:repo, :both)
        push!(
            results,
            bench_case(
                "repo_search",
                () -> WendaoArrow.gateway_repo_search(
                    client,
                    config.query;
                    limit = config.limit,
                );
                samples = config.samples,
            ),
        )
    end
    if config.route in (:knowledge, :both)
        push!(
            results,
            bench_case(
                "knowledge_search",
                () -> WendaoArrow.gateway_knowledge_search(
                    client,
                    config.query;
                    limit = config.limit,
                );
                samples = config.samples,
            ),
        )
    end
    print_results(results)
    return nothing
end

main(ARGS)

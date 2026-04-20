module WendaoArrowPackagedFlightBenchmarkServer

using WendaoArrow

const Arrow = WendaoArrow.Arrow
const Tables = WendaoArrow.Tables

Base.@kwdef struct PackagedFlightBenchmarkServerConfig
    host::String = WendaoArrow.DEFAULT_HOST
    port::Int = 0
    response_mode::Symbol = :echo
    large_doc_bytes::Int = 2 * 1024 * 1024
    processing_delay_ms::Int = 0
    max_active_requests::Int = max(Threads.nthreads() * 8, 32)
    request_capacity::Int = 16
    response_capacity::Int = 16
end

function parse_server_args(args::Vector{String})
    config_path = nothing
    host_override = nothing
    port_override = nothing

    response_mode = :echo
    large_doc_bytes = 2 * 1024 * 1024
    processing_delay_ms = 0
    max_active_requests = max(Threads.nthreads() * 8, 32)
    request_capacity = 16
    response_capacity = 16

    index = 1
    while index <= length(args)
        argument = args[index]
        if startswith(argument, "--config=")
            config_path = split(argument, "=", limit = 2)[2]
        elseif argument == "--config"
            index += 1
            index > length(args) && throw(ArgumentError("missing value for $(argument)"))
            config_path = args[index]
        elseif startswith(argument, "--host=")
            host_override = split(argument, "=", limit = 2)[2]
        elseif argument == "--host"
            index += 1
            index > length(args) && throw(ArgumentError("missing value for $(argument)"))
            host_override = args[index]
        elseif startswith(argument, "--port=")
            port_override = parse(Int, split(argument, "=", limit = 2)[2])
        elseif argument == "--port"
            index += 1
            index > length(args) && throw(ArgumentError("missing value for $(argument)"))
            port_override = parse(Int, args[index])
        elseif startswith(argument, "--response-mode=")
            response_mode = Symbol(split(argument, "=", limit = 2)[2])
        elseif startswith(argument, "--large-doc-bytes=")
            large_doc_bytes = parse(Int, split(argument, "=", limit = 2)[2])
        elseif startswith(argument, "--processing-delay-ms=")
            processing_delay_ms = parse(Int, split(argument, "=", limit = 2)[2])
        elseif startswith(argument, "--max-active-requests=")
            max_active_requests = parse(Int, split(argument, "=", limit = 2)[2])
        elseif startswith(argument, "--request-capacity=")
            request_capacity = parse(Int, split(argument, "=", limit = 2)[2])
        elseif startswith(argument, "--response-capacity=")
            response_capacity = parse(Int, split(argument, "=", limit = 2)[2])
        elseif argument in (
            "--response-mode",
            "--large-doc-bytes",
            "--processing-delay-ms",
            "--max-active-requests",
            "--request-capacity",
            "--response-capacity",
        )
            index += 1
            index > length(args) && throw(ArgumentError("missing value for $(argument)"))
            value = args[index]
            if argument == "--response-mode"
                response_mode = Symbol(value)
            elseif argument == "--large-doc-bytes"
                large_doc_bytes = parse(Int, value)
            elseif argument == "--processing-delay-ms"
                processing_delay_ms = parse(Int, value)
            elseif argument == "--max-active-requests"
                max_active_requests = parse(Int, value)
            elseif argument == "--request-capacity"
                request_capacity = parse(Int, value)
            else
                response_capacity = parse(Int, value)
            end
        else
            throw(ArgumentError("unsupported benchmark server argument: $(argument)"))
        end
        index += 1
    end

    base = isnothing(config_path) ?
           WendaoArrow.InterfaceConfig(host = WendaoArrow.DEFAULT_HOST, port = 0) :
           WendaoArrow.load_config(config_path)
    host = isnothing(host_override) ? base.host : host_override
    port = isnothing(port_override) ? base.port : port_override

    isempty(strip(host)) && throw(ArgumentError("benchmark server host must be non-empty"))
    port >= 0 || throw(ArgumentError("benchmark server port must be zero or greater"))
    large_doc_bytes > 0 ||
        throw(ArgumentError("benchmark server large_doc_bytes must be greater than zero"))
    processing_delay_ms >= 0 || throw(
        ArgumentError("benchmark server processing_delay_ms must be zero or greater"),
    )
    max_active_requests > 0 || throw(
        ArgumentError("benchmark server max_active_requests must be greater than zero"),
    )
    request_capacity > 0 || throw(
        ArgumentError("benchmark server request_capacity must be greater than zero"),
    )
    response_capacity > 0 || throw(
        ArgumentError("benchmark server response_capacity must be greater than zero"),
    )
    response_mode in (:echo, :large_response) || throw(
        ArgumentError(
            "benchmark server response_mode must be one of :echo or :large_response; got $(response_mode)",
        ),
    )

    return PackagedFlightBenchmarkServerConfig(
        host = host,
        port = port,
        response_mode = response_mode,
        large_doc_bytes = large_doc_bytes,
        processing_delay_ms = processing_delay_ms,
        max_active_requests = max_active_requests,
        request_capacity = request_capacity,
        response_capacity = response_capacity,
    )
end

function build_benchmark_processor(config::PackagedFlightBenchmarkServerConfig)
    large_doc_id = repeat("x", config.large_doc_bytes)
    return stream -> begin
        config.processing_delay_ms > 0 && sleep(config.processing_delay_ms / 1000)
        if config.response_mode == :large_response
            for _ in stream
            end
            return (
                doc_id = [large_doc_id],
                analyzer_score = [0.9],
                final_score = [0.9],
            )
        end

        doc_ids = String[]
        analyzer_scores = Float64[]
        final_scores = Float64[]
        for batch in stream
            columns = Tables.columntable(batch)
            for (doc_id, vector_score) in zip(columns.doc_id, columns.vector_score)
                score = Float64(vector_score)
                push!(doc_ids, doc_id)
                push!(analyzer_scores, score)
                push!(final_scores, score + 1.0)
            end
        end
        return (
            doc_id = doc_ids,
            analyzer_score = analyzer_scores,
            final_score = final_scores,
        )
    end
end

function start_server(config::PackagedFlightBenchmarkServerConfig)
    return WendaoArrow.serve_stream_flight(
        build_benchmark_processor(config);
        descriptor = WendaoArrow.flight_descriptor(("rerank",)),
        host = config.host,
        port = config.port,
        block = false,
        max_active_requests = config.max_active_requests,
        request_capacity = config.request_capacity,
        response_capacity = config.response_capacity,
    )
end

function main(args::Vector{String})
    config = parse_server_args(args)
    server = start_server(config)
    println("READY grpc://$(server.host):$(server.port)")
    flush(stdout)
    accept_task = getfield(server, :accept_task)
    isnothing(accept_task) || wait(accept_task)
    return nothing
end

end

using .WendaoArrowPackagedFlightBenchmarkServer

if abspath(PROGRAM_FILE) == @__FILE__
    WendaoArrowPackagedFlightBenchmarkServer.main(ARGS)
end

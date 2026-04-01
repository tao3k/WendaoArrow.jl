module WendaoArrowgRPCServerExt

using Arrow
using WendaoArrow
using gRPCServer

function WendaoArrow.flight_server(
    service::Arrow.Flight.Service;
    host::AbstractString = WendaoArrow.DEFAULT_HOST,
    port::Integer = WendaoArrow.DEFAULT_FLIGHT_PORT,
    enable_health_check::Bool = true,
    enable_reflection::Bool = true,
    kwargs...,
)
    server = gRPCServer.GRPCServer(
        String(host),
        Int(port);
        enable_health_check = enable_health_check,
        enable_reflection = enable_reflection,
        kwargs...,
    )
    gRPCServer.register!(server, service)
    return server
end

function WendaoArrow.serve_flight(
    processor::Function;
    host::AbstractString = WendaoArrow.DEFAULT_HOST,
    port::Integer = WendaoArrow.DEFAULT_FLIGHT_PORT,
    descriptor = WendaoArrow.flight_descriptor(),
    include_request_app_metadata::Bool = false,
    block::Bool = true,
    kwargs...,
)
    service = WendaoArrow.build_flight_service(
        processor;
        descriptor = descriptor,
        include_request_app_metadata = include_request_app_metadata,
    )
    server = WendaoArrow.flight_server(service; host = host, port = port, kwargs...)
    Base.run(server; block = block)
    return server
end

function WendaoArrow.serve_stream_flight(
    processor::Function;
    host::AbstractString = WendaoArrow.DEFAULT_HOST,
    port::Integer = WendaoArrow.DEFAULT_FLIGHT_PORT,
    descriptor = WendaoArrow.flight_descriptor(),
    include_request_app_metadata::Bool = false,
    block::Bool = true,
    kwargs...,
)
    service = WendaoArrow.build_stream_flight_service(
        processor;
        descriptor = descriptor,
        include_request_app_metadata = include_request_app_metadata,
    )
    server = WendaoArrow.flight_server(service; host = host, port = port, kwargs...)
    Base.run(server; block = block)
    return server
end

end

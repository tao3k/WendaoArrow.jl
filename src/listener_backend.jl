if isdefined(Arrow.Flight, :FlightServerBackendCapabilities)
    const FlightListenerBackendCapabilities =
        getfield(Arrow.Flight, :FlightServerBackendCapabilities)
else
    struct FlightListenerBackendCapabilities
        backend::Symbol
        request_streaming::Bool
        response_streaming::Bool
        response_trailers::Bool
        bidirectional_doexchange::Bool
        blockers::Vector{String}
    end
end

function _legacy_flight_listener_backend_error()
    throw(
        ArgumentError(
            "WendaoArrow Flight listener backend :purehttp2 has been retired; use the packaged Arrow listener surface via :grpcserver",
        ),
    )
end

function _fallback_flight_listener_backend_capabilities(backend::Symbol = :grpcserver)
    if backend == :purehttp2
        _legacy_flight_listener_backend_error()
    elseif backend == :grpcserver
        return FlightListenerBackendCapabilities(
            :grpcserver,
            false,
            false,
            false,
            false,
            String[
                "Arrow.jl ships the packaged Flight listener backend behind the optional gRPCServer.jl extension; load gRPCServer to activate it",
                "The legacy Arrow-owned PureHTTP2 listener surface has been retired; gRPCServer.jl now owns the packaged HTTP/2 transport",
            ],
        )
    elseif backend == :nghttp2
        return FlightListenerBackendCapabilities(
            :nghttp2,
            false,
            false,
            false,
            false,
            String[
                "Arrow.jl ships the nghttp2 backend behind the optional Nghttp2Wrapper.jl extension; load Nghttp2Wrapper to activate it",
                "The packaged live Flight listener backend now lives behind gRPCServer.jl",
            ],
        )
    end

    throw(
        ArgumentError(
            "Unsupported WendaoArrow Flight listener backend :$(backend); expected one of :grpcserver or :nghttp2",
        ),
    )
end

"""
Return the shared Arrow Flight server backend contract that WendaoArrow uses for
its Arrow-provided network listener wrappers.
"""
function flight_listener_backend_capabilities(backend::Symbol = :grpcserver)
    backend == :purehttp2 && _legacy_flight_listener_backend_error()
    return isdefined(Arrow.Flight, :flight_server_backend_capabilities) ?
           getfield(Arrow.Flight, :flight_server_backend_capabilities)(backend) :
           _fallback_flight_listener_backend_capabilities(backend)
end

"""
Return whether one backend satisfies the shared Arrow Flight server contract
that WendaoArrow requires for its Arrow-provided network listener wrappers.
"""
function flight_listener_backend_supported(backend::Symbol = :grpcserver)
    backend == :purehttp2 && _legacy_flight_listener_backend_error()
    return isdefined(Arrow.Flight, :flight_server_backend_supported) ?
           getfield(Arrow.Flight, :flight_server_backend_supported)(backend) :
    begin
        capabilities = _fallback_flight_listener_backend_capabilities(backend)
        capabilities.request_streaming &&
            capabilities.response_streaming &&
            capabilities.response_trailers &&
            capabilities.bidirectional_doexchange
    end
end

function require_flight_listener_backend(
    backend::Symbol = :grpcserver;
    subject::AbstractString = "WendaoArrow Flight listener",
)
    backend == :purehttp2 && _legacy_flight_listener_backend_error()
    if isdefined(Arrow.Flight, :require_flight_server_backend)
        return getfield(Arrow.Flight, :require_flight_server_backend)(
            backend;
            subject = subject,
        )
    end

    flight_listener_backend_supported(backend) && return nothing

    capabilities = _fallback_flight_listener_backend_capabilities(backend)
    blocker_message = join(capabilities.blockers, "; ")
    throw(
        ArgumentError(
            "$(subject) does not support backend :$(backend): $(blocker_message)",
        ),
    )
end

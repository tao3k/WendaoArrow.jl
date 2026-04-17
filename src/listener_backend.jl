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
            "WendaoArrow Flight listener backend :grpcserver has been retired; use :purehttp2",
        ),
    )
end

function _fallback_flight_listener_backend_capabilities(backend::Symbol = :purehttp2)
    if backend == :grpcserver
        _legacy_flight_listener_backend_error()
    elseif backend == :purehttp2
        return FlightListenerBackendCapabilities(
            :purehttp2,
            true,
            true,
            true,
            true,
            String[],
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
                "PureHTTP2 remains the only default packaged Flight listener backend",
            ],
        )
    end

    throw(
        ArgumentError(
            "Unsupported WendaoArrow Flight listener backend :$(backend); expected one of :purehttp2 or :nghttp2",
        ),
    )
end

"""
Return the shared Arrow Flight server backend contract that WendaoArrow uses for
its packaged network listener surface.
"""
function flight_listener_backend_capabilities(backend::Symbol = :purehttp2)
    backend == :grpcserver && _legacy_flight_listener_backend_error()
    return isdefined(Arrow.Flight, :flight_server_backend_capabilities) ?
           getfield(Arrow.Flight, :flight_server_backend_capabilities)(backend) :
           _fallback_flight_listener_backend_capabilities(backend)
end

"""
Return whether one backend satisfies the shared Arrow Flight server contract
that WendaoArrow requires for its packaged network listener.
"""
function flight_listener_backend_supported(backend::Symbol = :purehttp2)
    backend == :grpcserver && _legacy_flight_listener_backend_error()
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
    backend::Symbol = :purehttp2;
    subject::AbstractString = "WendaoArrow Flight listener",
)
    backend == :grpcserver && _legacy_flight_listener_backend_error()
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

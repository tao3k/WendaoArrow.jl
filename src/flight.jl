const DEFAULT_FLIGHT_DESCRIPTOR_PATH = ("wendao", "arrow", "exchange")

"""
Prepared Flight `DoExchange` request over one source, descriptor, and header
set.
"""
struct FlightExchangeRequest{S}
    source::S
    descriptor::Arrow.Flight.Protocol.FlightDescriptor
    headers::Vector{Pair{String,String}}
end

function flight_descriptor(path = DEFAULT_FLIGHT_DESCRIPTOR_PATH)
    return Arrow.Flight.pathdescriptor(path)
end

"""
Return one prepared Flight `DoExchange` request over a source plus descriptor
and headers.
"""
function flight_exchange_request(
    source;
    descriptor::Union{Nothing,Arrow.Flight.Protocol.FlightDescriptor} = nothing,
    route::Union{Nothing,AbstractString} = nothing,
    headers::AbstractVector{<:Pair} = Pair{String,String}[],
    subject::AbstractString = "WendaoArrow Flight exchange request",
)
    isnothing(descriptor) ⊻ isnothing(route) ||
        error("$(subject) must specify exactly one of descriptor or route")
    normalized_descriptor =
        isnothing(descriptor) ? flight_route_descriptor(route; subject = subject) :
        descriptor
    normalized_headers = _gateway_header_pairs(headers; subject = subject)
    return FlightExchangeRequest(source, normalized_descriptor, normalized_headers)
end

"""
Invoke one prepared Flight `DoExchange` request against an in-process Flight
service and decode the response as an Arrow table.
"""
function flight_exchange_table(
    service::Arrow.Flight.Service,
    context::Arrow.Flight.ServerCallContext,
    request::FlightExchangeRequest;
    convert::Bool = true,
    include_app_metadata::Bool = false,
)
    return Arrow.Flight.table(
        service,
        context,
        request.source;
        descriptor = request.descriptor,
        convert = convert,
        include_app_metadata = include_app_metadata,
    )
end

"""
Invoke one prepared Flight `DoExchange` request against a Flight client and
decode the response as an Arrow table.
"""
function flight_exchange_table(
    client::Arrow.Flight.Client,
    request::FlightExchangeRequest;
    convert::Bool = true,
    include_app_metadata::Bool = false,
)
    req, response = Arrow.Flight.doexchange(
        client,
        request.source;
        descriptor = request.descriptor,
        headers = request.headers,
    )
    result = Arrow.Flight.table(
        response;
        convert = convert,
        include_app_metadata = include_app_metadata,
    )
    wait(req)
    return result
end

_flight_request_table(request) = request
_flight_request_table(request::NamedTuple{(:table, :app_metadata)}) = request.table

function _decode_flight_table(
    messages;
    include_request_app_metadata::Bool = false,
    expected_schema_version::AbstractString = DEFAULT_SCHEMA_VERSION,
)
    request = Arrow.Flight.table(
        messages;
        convert = true,
        include_app_metadata = include_request_app_metadata,
    )
    table = _flight_request_table(request)
    schema = Tables.schema(table)
    if isempty(schema.names)
        throw(
            ArgumentError("Arrow Flight exchange request must contain at least one column"),
        )
    end
    require_schema_version(
        table;
        subject = "Arrow Flight exchange request",
        expected = expected_schema_version,
    )
    return request
end

function _decode_flight_stream(
    messages;
    include_request_app_metadata::Bool = false,
    expected_schema_version::AbstractString = DEFAULT_SCHEMA_VERSION,
)
    stream = Arrow.Flight.stream(
        messages;
        convert = true,
        include_app_metadata = include_request_app_metadata,
    )
    return _validated_stream(
        stream,
        "Arrow Flight exchange request must contain at least one record batch";
        subject = "Arrow Flight exchange request",
        expected_schema_version = expected_schema_version,
    )
end

function build_flight_exchange_service(
    decoder::Function,
    processor::Function;
    descriptor = flight_descriptor(),
    include_request_app_metadata::Bool = false,
    expected_schema_version::AbstractString = DEFAULT_SCHEMA_VERSION,
)
    return Arrow.Flight.exchangeservice(
        function (incoming_messages, request_descriptor, _)
            input_value = try
                decoder(
                    incoming_messages;
                    include_request_app_metadata = include_request_app_metadata,
                    expected_schema_version = expected_schema_version,
                )
            catch error
                @error "WendaoArrow failed to decode Arrow Flight exchange request" exception =
                    (error, catch_backtrace()) descriptor_path =
                    request_descriptor.path
                rethrow()
            end

            output_table = try
                processor(input_value)
            catch error
                @error "WendaoArrow Flight processor failed" exception =
                    (error, catch_backtrace()) descriptor_path =
                    request_descriptor.path
                rethrow()
            end

            return output_table
        end;
        descriptor = descriptor,
        writer = function (response, output_table, request_descriptor, _)
            try
                return Arrow.Flight.putflightdata!(
                    response,
                    output_table;
                    metadata = _response_schema_metadata(
                        output_table;
                        schema_version = expected_schema_version,
                    ),
                )
            catch error
                @error "WendaoArrow failed to encode Arrow Flight processor output" exception =
                    (error, catch_backtrace()) descriptor_path = request_descriptor.path
                rethrow()
            end
        end,
    )
end

function build_flight_service(
    processor::Function;
    descriptor = flight_descriptor(),
    include_request_app_metadata::Bool = false,
    expected_schema_version::AbstractString = DEFAULT_SCHEMA_VERSION,
)
    return build_flight_exchange_service(
        _decode_flight_table,
        processor;
        descriptor = descriptor,
        include_request_app_metadata = include_request_app_metadata,
        expected_schema_version = expected_schema_version,
    )
end

function build_stream_flight_service(
    processor::Function;
    descriptor = flight_descriptor(),
    include_request_app_metadata::Bool = false,
    expected_schema_version::AbstractString = DEFAULT_SCHEMA_VERSION,
)
    return build_flight_exchange_service(
        _decode_flight_stream,
        processor;
        descriptor = descriptor,
        include_request_app_metadata = include_request_app_metadata,
        expected_schema_version = expected_schema_version,
    )
end

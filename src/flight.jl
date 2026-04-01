const DEFAULT_FLIGHT_DESCRIPTOR_PATH = ("wendao", "arrow", "exchange")

function flight_descriptor(path = DEFAULT_FLIGHT_DESCRIPTOR_PATH)
    return Arrow.Flight.pathdescriptor(path)
end

_flight_request_table(request) = request
_flight_request_table(request::NamedTuple{(:table, :app_metadata)}) = request.table

function _decode_flight_table(messages; include_request_app_metadata::Bool = false)
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
    require_schema_version(table; subject = "Arrow Flight exchange request")
    return request
end

function _decode_flight_stream(messages; include_request_app_metadata::Bool = false)
    stream = Arrow.Flight.stream(
        messages;
        convert = true,
        include_app_metadata = include_request_app_metadata,
    )
    return _validated_stream(
        stream,
        "Arrow Flight exchange request must contain at least one record batch";
        subject = "Arrow Flight exchange request",
    )
end

function build_flight_exchange_service(
    decoder::Function,
    processor::Function;
    descriptor = flight_descriptor(),
    include_request_app_metadata::Bool = false,
)
    return Arrow.Flight.exchangeservice(
        function (incoming_messages, request_descriptor, _)
            input_value = try
                decoder(
                    incoming_messages;
                    include_request_app_metadata = include_request_app_metadata,
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
                    metadata = _response_schema_metadata(output_table),
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
)
    return build_flight_exchange_service(
        _decode_flight_table,
        processor;
        descriptor = descriptor,
        include_request_app_metadata = include_request_app_metadata,
    )
end

function build_stream_flight_service(
    processor::Function;
    descriptor = flight_descriptor(),
    include_request_app_metadata::Bool = false,
)
    return build_flight_exchange_service(
        _decode_flight_stream,
        processor;
        descriptor = descriptor,
        include_request_app_metadata = include_request_app_metadata,
    )
end

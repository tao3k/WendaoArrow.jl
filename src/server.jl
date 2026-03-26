function build_handler(processor::Function; config::InterfaceConfig = InterfaceConfig())
    normalized_route = config.route
    normalized_health_route = config.health_route
    return function (req::HTTP.Request)
        target = String(req.target)
        if target == normalized_health_route
            return health_response()
        end
        if target != normalized_route
            return HTTP.Response(404, "route not found")
        end

        input_table = try
            decode_ipc(req.body)
        catch error
            @error "WendaoArrow failed to decode Arrow IPC request" exception = (error, catch_backtrace()) route = normalized_route
            return invalid_request_response()
        end

        output_table = try
            processor(input_table)
        catch error
            @error "WendaoArrow processor failed" exception = (error, catch_backtrace()) route = normalized_route
            return processor_failure_response()
        end

        response_body = try
            encode_ipc(output_table)
        catch error
            @error "WendaoArrow failed to encode processor output" exception = (error, catch_backtrace()) route = normalized_route
            return processor_failure_response()
        end

        return HTTP.Response(200, ["Content-Type" => config.content_type], body = response_body)
    end
end

function serve(
    processor::Function;
    config::InterfaceConfig = InterfaceConfig(),
)
    handler = build_handler(processor; config = config)
    return HTTP.serve(handler, config.host, config.port)
end

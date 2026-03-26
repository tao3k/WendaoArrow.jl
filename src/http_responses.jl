function health_response()::HTTP.Response
    return HTTP.Response(
        200,
        ["Content-Type" => JSON_CONTENT_TYPE],
        body = "{\"status\":\"ok\"}",
    )
end

function invalid_request_response()::HTTP.Response
    return HTTP.Response(
        400,
        ["Content-Type" => JSON_CONTENT_TYPE],
        body = "{\"error\":\"invalid_arrow_ipc_request\"}",
    )
end

function processor_failure_response()::HTTP.Response
    return HTTP.Response(
        500,
        ["Content-Type" => JSON_CONTENT_TYPE],
        body = "{\"error\":\"processor_failed\"}",
    )
end

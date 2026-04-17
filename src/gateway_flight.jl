const DEFAULT_GATEWAY_REPO_SEARCH_ROUTE = "/search/repos/main"
const DEFAULT_GATEWAY_KNOWLEDGE_SEARCH_ROUTE = "/search/knowledge"
const DEFAULT_GATEWAY_RESULT_LIMIT = 10
const DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH = 256 * 1024 * 1024

function _gateway_required_string(
    value,
    label::AbstractString;
    subject::AbstractString = "WendaoArrow gateway Flight request",
)
    value isa AbstractString || throw(
        ArgumentError(
            "$(subject) $(label) must be a string; got $(repr(value))::$(typeof(value))",
        ),
    )
    text = String(value)
    isempty(strip(text)) && throw(ArgumentError("$(subject) $(label) must be non-empty"))
    return text
end

function _gateway_limit(
    limit::Integer;
    subject::AbstractString = "WendaoArrow gateway Flight request",
)
    limit > 0 ||
        throw(ArgumentError("$(subject) limit must be greater than zero; got $(limit)"))
    return Int(limit)
end

function _gateway_header_pairs(
    headers::AbstractVector{<:Pair};
    subject::AbstractString = "WendaoArrow gateway Flight request",
)
    return Pair{String,String}[
        _gateway_required_string(first(header), "header key"; subject = subject) =>
            _gateway_required_string(last(header), "header value"; subject = subject) for
        header in headers
    ]
end

function flight_route_descriptor(
    route::AbstractString;
    subject::AbstractString = "WendaoArrow Flight descriptor",
)
    normalized = _gateway_required_string(route, "route"; subject = subject)
    segments = filter(!isempty, split(chopprefix(normalized, "/"), "/"))
    isempty(segments) &&
        throw(ArgumentError("$(subject) route must contain at least one segment"))
    return Arrow.Flight.pathdescriptor(segments)
end

function gateway_flight_descriptor(route::AbstractString)
    return flight_route_descriptor(route; subject = "WendaoArrow gateway Flight descriptor")
end

function flight_schema_headers(;
    schema_version::AbstractString = DEFAULT_SCHEMA_VERSION,
    headers::AbstractVector{<:Pair} = Pair{String,String}[],
    subject::AbstractString = "WendaoArrow Flight request",
)
    result = Pair{String,String}["x-wendao-schema-version"=>_gateway_required_string(
        schema_version,
        "schema_version",
        subject = subject,
    ),]
    append!(result, _gateway_header_pairs(headers; subject = subject))
    return result
end

function _gateway_base_headers(;
    schema_version::AbstractString = DEFAULT_GATEWAY_SCHEMA_VERSION,
    headers::AbstractVector{<:Pair} = Pair{String,String}[],
)
    return flight_schema_headers(
        schema_version = schema_version,
        headers = headers,
        subject = "WendaoArrow gateway Flight request",
    )
end

function gateway_repo_search_headers(
    query::AbstractString;
    limit::Integer = DEFAULT_GATEWAY_RESULT_LIMIT,
    schema_version::AbstractString = DEFAULT_GATEWAY_SCHEMA_VERSION,
    headers::AbstractVector{<:Pair} = Pair{String,String}[],
)
    result = _gateway_base_headers(; schema_version = schema_version, headers = headers)
    push!(result, "x-wendao-repo-search-query" => _gateway_required_string(query, "query"))
    push!(result, "x-wendao-repo-search-limit" => string(_gateway_limit(limit)))
    return result
end

function gateway_knowledge_search_headers(
    query::AbstractString;
    limit::Integer = DEFAULT_GATEWAY_RESULT_LIMIT,
    schema_version::AbstractString = DEFAULT_GATEWAY_SCHEMA_VERSION,
    intent::Union{Nothing,AbstractString} = nothing,
    repo::Union{Nothing,AbstractString} = nothing,
    headers::AbstractVector{<:Pair} = Pair{String,String}[],
)
    result = _gateway_base_headers(; schema_version = schema_version, headers = headers)
    push!(result, "x-wendao-search-query" => _gateway_required_string(query, "query"))
    push!(result, "x-wendao-search-limit" => string(_gateway_limit(limit)))
    if !isnothing(intent)
        push!(
            result,
            "x-wendao-search-intent" => _gateway_required_string(intent, "intent"),
        )
    end
    if !isnothing(repo)
        push!(result, "x-wendao-search-repo" => _gateway_required_string(repo, "repo"))
    end
    return result
end

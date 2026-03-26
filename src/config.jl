Base.@kwdef struct InterfaceConfig
    content_type::String = CONTENT_TYPE
    route::String = DEFAULT_ROUTE
    health_route::String = DEFAULT_HEALTH_ROUTE
    host::String = DEFAULT_HOST
    port::Int = DEFAULT_PORT
end

function load_config(path::AbstractString)::InterfaceConfig
    parsed = TOML.parsefile(path)
    section = get(parsed, "interface", Dict{String, Any}())
    return InterfaceConfig(
        content_type = string(get(section, "content_type", CONTENT_TYPE)),
        route = string(get(section, "route", DEFAULT_ROUTE)),
        health_route = string(get(section, "health_route", DEFAULT_HEALTH_ROUTE)),
        host = string(get(section, "host", DEFAULT_HOST)),
        port = Int(get(section, "port", DEFAULT_PORT)),
    )
end

function config_from_args(args::Vector{String})::InterfaceConfig
    config_path = nothing
    overrides = Dict{String, String}()
    index = 1

    while index <= length(args)
        argument = args[index]
        if startswith(argument, "--config=")
            config_path = split(argument, "=", limit = 2)[2]
        elseif argument == "--config"
            index += 1
            config_path = args[index]
        elseif startswith(argument, "--host=")
            overrides["host"] = split(argument, "=", limit = 2)[2]
        elseif argument == "--host"
            index += 1
            overrides["host"] = args[index]
        elseif startswith(argument, "--port=")
            overrides["port"] = split(argument, "=", limit = 2)[2]
        elseif argument == "--port"
            index += 1
            overrides["port"] = args[index]
        elseif startswith(argument, "--route=")
            overrides["route"] = split(argument, "=", limit = 2)[2]
        elseif argument == "--route"
            index += 1
            overrides["route"] = args[index]
        elseif startswith(argument, "--health-route=")
            overrides["health_route"] = split(argument, "=", limit = 2)[2]
        elseif argument == "--health-route"
            index += 1
            overrides["health_route"] = args[index]
        elseif startswith(argument, "--content-type=")
            overrides["content_type"] = split(argument, "=", limit = 2)[2]
        elseif argument == "--content-type"
            index += 1
            overrides["content_type"] = args[index]
        else
            throw(ArgumentError("unsupported argument: $argument"))
        end
        index += 1
    end

    base = isnothing(config_path) ? InterfaceConfig() : load_config(config_path)
    return InterfaceConfig(
        content_type = get(overrides, "content_type", base.content_type),
        route = get(overrides, "route", base.route),
        health_route = get(overrides, "health_route", base.health_route),
        host = get(overrides, "host", base.host),
        port = parse(Int, get(overrides, "port", string(base.port))),
    )
end

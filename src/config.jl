Base.@kwdef struct InterfaceConfig
    host::String = DEFAULT_HOST
    port::Int = DEFAULT_FLIGHT_PORT
end

function load_config(path::AbstractString)::InterfaceConfig
    parsed = TOML.parsefile(path)
    section = get(parsed, "interface", Dict{String,Any}())
    return InterfaceConfig(
        host = string(get(section, "host", DEFAULT_HOST)),
        port = Int(get(section, "port", DEFAULT_FLIGHT_PORT)),
    )
end

function config_from_args(args::Vector{String})::InterfaceConfig
    config_path = nothing
    overrides = Dict{String,String}()
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
        else
            throw(ArgumentError("unsupported argument: $argument"))
        end
        index += 1
    end

    base = isnothing(config_path) ? InterfaceConfig() : load_config(config_path)
    return InterfaceConfig(
        host = get(overrides, "host", base.host),
        port = parse(Int, get(overrides, "port", string(base.port))),
    )
end

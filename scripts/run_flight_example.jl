using Pkg

const SCRIPT_ROOT = @__DIR__
const WENDAO_ROOT = normpath(joinpath(SCRIPT_ROOT, ".."))
const ARROW_ROOT = normpath(joinpath(WENDAO_ROOT, "..", "arrow-julia"))
const ARROWTYPES_ROOT = joinpath(ARROW_ROOT, "src", "ArrowTypes")

function maybe_git_root(path::AbstractString)
    try
        return readchomp(
            pipeline(`git -C $path rev-parse --show-toplevel`; stderr = devnull),
        )
    catch
        return nothing
    end
end

function flight_roots(path::AbstractString)
    roots = String[]
    current = abspath(path)
    while true
        root = maybe_git_root(current)
        if !isnothing(root) && root ∉ roots
            push!(roots, root)
        end
        parent = dirname(current)
        parent == current && break
        current = parent
    end
    return roots
end

function path_within_root(path::AbstractString, root::AbstractString)
    normalized_path = splitpath(normpath(abspath(path)))
    normalized_root = splitpath(normpath(abspath(root)))
    length(normalized_path) >= length(normalized_root) || return false
    return normalized_path[1:length(normalized_root)] == normalized_root
end

function allowed_example_roots()
    roots = String[WENDAO_ROOT]
    for root in flight_roots(SCRIPT_ROOT)
        push!(roots, joinpath(root, ".cache"))
    end
    return unique(normpath.(abspath.(roots)))
end

function resolve_example_target(path_arg::AbstractString)
    candidate =
        isabspath(path_arg) ? normpath(path_arg) : normpath(joinpath(WENDAO_ROOT, path_arg))
    isfile(candidate) || error("Flight example does not exist: $candidate")
    any(path_within_root(candidate, root) for root in allowed_example_roots()) || error(
        "Flight example path must stay within WendaoArrow or project cache roots: $candidate",
    )
    return candidate
end

function maybe_locate_grpcserver()
    if haskey(ENV, "WENDAO_FLIGHT_GRPCSERVER_PATH")
        candidate = abspath(ENV["WENDAO_FLIGHT_GRPCSERVER_PATH"])
        isdir(candidate) ||
            error("WENDAO_FLIGHT_GRPCSERVER_PATH does not exist: $candidate")
        return candidate
    end
    for root in flight_roots(SCRIPT_ROOT)
        candidate = joinpath(root, ".cache", "vendor", "gRPCServer.jl")
        isdir(candidate) && return candidate
    end
    return nothing
end

function activate_flight_env()
    temp_env = mktempdir()
    Pkg.activate(temp_env)
    Pkg.develop(PackageSpec(path = WENDAO_ROOT))
    Pkg.develop(PackageSpec(path = ARROW_ROOT))
    Pkg.develop(PackageSpec(path = ARROWTYPES_ROOT))
    grpcserver = maybe_locate_grpcserver()
    if !isnothing(grpcserver)
        Pkg.develop(PackageSpec(path = grpcserver))
    else
        error(
            "Could not locate vendored gRPCServer.jl. " *
            "Set WENDAO_FLIGHT_GRPCSERVER_PATH to an explicit checkout path " *
            "or add .cache/vendor/gRPCServer.jl under the repository root.",
        )
    end
    Pkg.add("Tables")
    Pkg.instantiate()
    return temp_env
end

function main(args::Vector{String})
    isempty(args) && error("expected a relative Flight example path")
    target = resolve_example_target(args[1])

    activate_flight_env()
    empty!(ARGS)
    append!(ARGS, args[2:end])
    return include(target)
end

main(copy(ARGS))

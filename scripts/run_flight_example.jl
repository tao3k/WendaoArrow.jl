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

function locate_grpcserver()
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
    error(
        "Could not locate vendored gRPCServer.jl. " *
        "Set WENDAO_FLIGHT_GRPCSERVER_PATH to an explicit checkout path.",
    )
end

function activate_flight_env()
    temp_env = mktempdir()
    Pkg.activate(temp_env)
    Pkg.develop(PackageSpec(path = WENDAO_ROOT))
    Pkg.develop(PackageSpec(path = ARROW_ROOT))
    Pkg.develop(PackageSpec(path = ARROWTYPES_ROOT))
    Pkg.develop(PackageSpec(path = locate_grpcserver()))
    Pkg.add("Tables")
    Pkg.instantiate()
    return temp_env
end

function main(args::Vector{String})
    isempty(args) && error("expected a relative Flight example path")
    target = normpath(joinpath(WENDAO_ROOT, args[1]))
    startswith(target, WENDAO_ROOT) ||
        error("Flight example path must stay within WendaoArrow: $target")
    isfile(target) || error("Flight example does not exist: $target")

    activate_flight_env()
    empty!(ARGS)
    append!(ARGS, args[2:end])
    return include(target)
end

main(copy(ARGS))

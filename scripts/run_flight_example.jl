import Pkg

const SCRIPT_ROOT = @__DIR__
const WENDAO_ROOT = normpath(joinpath(SCRIPT_ROOT, ".."))

function flight_env_path()
    if haskey(ENV, "WENDAO_ARROW_FLIGHT_ENV")
        path = abspath(ENV["WENDAO_ARROW_FLIGHT_ENV"])
        mkpath(path)
        return path
    end

    if haskey(ENV, "PRJ_CACHE_HOME")
        parent = joinpath(abspath(ENV["PRJ_CACHE_HOME"]), "julia")
        mkpath(parent)
        path = joinpath(parent, "wendaoarrow-flight-env-$(getpid())-$(Base.time_ns())")
        mkpath(path)
        return path
    end

    return mktempdir()
end

function maybe_workspace_arrow_checkout()
    candidates = String[]
    if haskey(ENV, "PRJ_ROOT")
        push!(candidates, ENV["PRJ_ROOT"])
    end
    for root in flight_roots(WENDAO_ROOT)
        push!(candidates, root)
    end
    for candidate_root in unique(normpath.(abspath.(candidates)))
        candidate = normpath(joinpath(candidate_root, ".data", "arrow-julia"))
        isfile(joinpath(candidate, "Project.toml")) || continue
        isfile(joinpath(candidate, "src", "ArrowTypes", "Project.toml")) || continue
        return candidate
    end
    return nothing
end

function maybe_bootstrap_local_arrow_checkout()
    env_path = flight_env_path()

    if haskey(ENV, "WENDAO_ARROW_FLIGHT_ENV")
        for stale_file in ("Project.toml", "Manifest.toml")
            candidate = joinpath(env_path, stale_file)
            isfile(candidate) && rm(candidate; force = true)
        end
    end

    Pkg.activate(env_path)
    Pkg.develop([Pkg.PackageSpec(path = WENDAO_ROOT)])

    local_arrow_checkout = maybe_workspace_arrow_checkout()
    if !isnothing(local_arrow_checkout)
        Pkg.develop(
            [
                Pkg.PackageSpec(path = local_arrow_checkout),
                Pkg.PackageSpec(path = joinpath(local_arrow_checkout, "src", "ArrowTypes")),
            ],
        )
    end
    Pkg.add("Tables")
    Pkg.instantiate()
    return local_arrow_checkout
end

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

function main(args::Vector{String})
    isempty(args) && error("expected a relative Flight example path")
    target = resolve_example_target(args[1])
    maybe_bootstrap_local_arrow_checkout()

    empty!(ARGS)
    append!(ARGS, args[2:end])
    return include(target)
end

main(copy(ARGS))

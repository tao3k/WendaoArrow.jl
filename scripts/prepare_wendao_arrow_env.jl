import Pkg

const SCRIPT_ROOT = @__DIR__
const WENDAO_ROOT = normpath(joinpath(SCRIPT_ROOT, ".."))
const LOCAL_ARROW_ENV = "WENDAO_ARROW_LOCAL_ARROW_PATH"
const BOOTSTRAP_ENV = "WENDAO_ARROW_BOOTSTRAP_ENV"

function valid_arrow_checkout(path::AbstractString)
    return isfile(joinpath(path, "Project.toml")) &&
           isfile(joinpath(path, "src", "ArrowTypes", "Project.toml"))
end

function candidate_arrow_checkouts()
    candidates = String[]

    if haskey(ENV, LOCAL_ARROW_ENV)
        push!(candidates, abspath(ENV[LOCAL_ARROW_ENV]))
    end

    push!(candidates, normpath(joinpath(dirname(WENDAO_ROOT), "arrow-julia")))

    if haskey(ENV, "PRJ_ROOT")
        push!(candidates, normpath(joinpath(ENV["PRJ_ROOT"], ".data", "arrow-julia")))
    end

    return unique(candidates)
end

function maybe_local_arrow_checkout()
    for candidate in candidate_arrow_checkouts()
        valid_arrow_checkout(candidate) && return candidate
    end
    return nothing
end

env_path = get(ENV, BOOTSTRAP_ENV, mktempdir())
Pkg.activate(env_path)
Pkg.develop([Pkg.PackageSpec(path = WENDAO_ROOT)])

local_arrow_checkout = maybe_local_arrow_checkout()
if !isnothing(local_arrow_checkout)
    Pkg.develop(
        [
            Pkg.PackageSpec(path = local_arrow_checkout),
            Pkg.PackageSpec(path = joinpath(local_arrow_checkout, "src", "ArrowTypes")),
        ];
        preserve = Pkg.PRESERVE_DIRECT,
    )
end

Pkg.resolve()
Pkg.instantiate()
Pkg.build("WendaoArrow")

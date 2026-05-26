const REQUIRED_CDATA_SURFACES =
    (:exporttable, :exporttable!, :importtable, :exportstream, :exportstream!, :importstream)

"""
    cdata_interface_capabilities()

Return the upstream `Arrow.CData` capability surface consumed by WendaoArrow.

WendaoArrow does not reimplement the Arrow C Data Interface. It verifies that
the active Arrow.jl dependency exposes the same same-process C Data/C Stream
ABI that Rust embedding or another in-process host can call through
`ArrowSchema`, `ArrowArray`, and `ArrowArrayStream` pointers.
"""
function cdata_interface_capabilities()
    has_cdata = isdefined(Arrow, :CData)
    cdata_module = has_cdata ? getfield(Arrow, :CData) : nothing
    surfaces = Dict(
        String(name) => has_cdata && isdefined(cdata_module, name) for
        name in REQUIRED_CDATA_SURFACES
    )
    header_available = has_cdata &&
                       isdefined(cdata_module, :header_path) &&
                       isfile(cdata_module.header_path())
    return (
        available = has_cdata && all(values(surfaces)) && header_available,
        has_cdata = has_cdata,
        surfaces = surfaces,
        header_available = header_available,
        header_path = header_available ? cdata_module.header_path() : "",
        same_process_only = true,
    )
end

"""
    require_cdata_interface(; subject="WendaoArrow C Data bridge")

Require the upstream `Arrow.CData` surface needed for same-process Rust/Julia
zero-copy experiments.
"""
function require_cdata_interface(; subject::AbstractString = "WendaoArrow C Data bridge")
    capabilities = cdata_interface_capabilities()
    capabilities.available && return capabilities
    missing = [
        name for (name, present) in sort(collect(capabilities.surfaces); by = first) if !present
    ]
    header_issue = capabilities.header_available ? String[] : ["header_path"]
    throw(
        ArgumentError(
            "$(subject) requires an Arrow.jl revision with Arrow.CData, surfaces $(join(vcat(missing, header_issue), ", ")), and include/arrow_julia_cdata.h",
        ),
    )
end

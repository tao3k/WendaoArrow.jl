using Test
using Tables
using WendaoArrow

const Arrow = WendaoArrow.Arrow
const DateTime = WendaoArrow.Dates.DateTime
const VALID_SCHEMA_VERSION_METADATA =
    ["wendao.schema_version" => WendaoArrow.DEFAULT_SCHEMA_VERSION]

function sample_table()
    return (doc_id = ["doc-a", "doc-b"], vector_score = [0.9, 0.5])
end

function sample_batches()
    return (
        (doc_id = ["doc-a", "doc-b"], vector_score = [0.9, 0.5]),
        (doc_id = ["doc-c"], vector_score = [0.25]),
    )
end

function arrow_ipc_bytes(table_like; metadata = nothing, colmetadata = nothing)
    io = IOBuffer()
    if isnothing(metadata) && isnothing(colmetadata)
        Arrow.write(io, table_like)
    else
        kwargs = Pair{Symbol,Any}[]
        !isnothing(metadata) && push!(kwargs, :metadata => metadata)
        !isnothing(colmetadata) && push!(kwargs, :colmetadata => colmetadata)
        Arrow.write(io, table_like; kwargs...)
    end
    return take!(io)
end

function arrow_table_with_metadata(table_like; metadata, colmetadata = nothing)
    return Arrow.Table(
        IOBuffer(
            arrow_ipc_bytes(table_like; metadata = metadata, colmetadata = colmetadata),
        );
        convert = true,
    )
end

function column_metadata(table_like, column::Symbol)
    return Arrow.getmetadata(Tables.getcolumn(table_like, column))
end

const SCHEMA_TABLE_IPC_ALIGNMENT = 64

struct SchemaPartitionedTable{T}
    source::T
    metadata::Vector{Pair{String,String}}
end

function _first_partition(source)
    state = iterate(Tables.partitions(source))
    state === nothing && throw(
        ArgumentError(
            "WendaoArrow partitioned schema table requires at least one partition",
        ),
    )
    return first(state)
end

Tables.partitions(table::SchemaPartitionedTable) = Tables.partitions(table.source)
Tables.schema(table::SchemaPartitionedTable) = Tables.schema(_first_partition(table.source))
Tables.columns(table::SchemaPartitionedTable) = Tables.columntable(table)

function Tables.columntable(table::SchemaPartitionedTable)
    partitions = collect(Tables.partitions(table.source))
    isempty(partitions) && return NamedTuple()
    length(partitions) == 1 && return Tables.columntable(only(partitions))

    columns_per_partition = map(Tables.columntable, partitions)
    names = propertynames(first(columns_per_partition))
    merged_columns = Tuple(
        reduce(
            vcat,
            (collect(getproperty(columns, name)) for columns in columns_per_partition),
        ) for name in names
    )
    return NamedTuple{names}(merged_columns)
end

function schema_version_metadata(; schema_version::AbstractString = DEFAULT_SCHEMA_VERSION)
    return ["wendao.schema_version" => _normalized_schema_version(schema_version)]
end

function merge_schema_metadata(
    metadata_sources...;
    schema_version::AbstractString = DEFAULT_SCHEMA_VERSION,
    subject::AbstractString = "WendaoArrow schema metadata",
)
    merged = Dict{String,String}()
    for metadata in metadata_sources
        metadata === nothing && continue
        for entry in _schema_metadata_entries(metadata)
            entry isa Pair || throw(
                ArgumentError(
                    "$(subject) entries must be Pair(key => value); got $(repr(entry))::$(typeof(entry))",
                ),
            )
            key = _normalized_schema_metadata_text(first(entry); label = "$(subject) key")
            value = _normalized_schema_metadata_text(
                last(entry);
                label = "$(subject) value for $(key)",
            )
            merged[key] = value
        end
    end
    merged["wendao.schema_version"] = _normalized_schema_version(schema_version)
    return collect(merged)
end

function schema_table(
    table_like;
    schema_version::AbstractString = DEFAULT_SCHEMA_VERSION,
    metadata = nothing,
    colmetadata = nothing,
    convert::Bool = true,
)
    io = IOBuffer()
    kwargs = Pair{Symbol,Any}[:metadata=>merge_schema_metadata(
        schema_metadata(table_like),
        metadata;
        schema_version = schema_version,
        subject = "WendaoArrow schema table metadata",
    ),]
    if table_like isa Tables.Partitioner
        return SchemaPartitionedTable(table_like, kwargs[1].second)
    end
    !isnothing(colmetadata) && push!(kwargs, :colmetadata => colmetadata)
    push!(kwargs, :alignment => SCHEMA_TABLE_IPC_ALIGNMENT)
    Arrow.write(io, table_like; kwargs...)
    return Arrow.Table(IOBuffer(take!(io)); convert = convert)
end

function _response_schema_metadata(
    table_like;
    schema_version::AbstractString = DEFAULT_SCHEMA_VERSION,
)
    return merge_schema_metadata(
        schema_metadata(table_like);
        schema_version = schema_version,
    )
end

function schema_metadata(table)
    metadata = Arrow.getmetadata(table)
    return metadata === nothing ? Dict{String,String}() : metadata
end

schema_metadata(table::SchemaPartitionedTable) = Dict{String,String}(table.metadata)

_validated_batch_table(batch) = batch
_validated_batch_table(batch::NamedTuple{(:table, :app_metadata)}) = batch.table

struct ValidatedStream{S,B,State}
    inner::S
    first_batch::B
    next_state::State
end

Tables.partitions(stream::ValidatedStream) = stream
Tables.schema(stream::ValidatedStream) =
    Tables.schema(_validated_batch_table(stream.first_batch))
Base.IteratorSize(::Type{<:ValidatedStream}) = Base.SizeUnknown()
Base.eltype(::Type{ValidatedStream{S,B,State}}) where {S,B,State} = B
Base.iterate(stream::ValidatedStream) = (stream.first_batch, stream.next_state)
Base.iterate(stream::ValidatedStream, state) = iterate(stream.inner, state)
schema_metadata(stream::ValidatedStream) =
    schema_metadata(_validated_batch_table(stream.first_batch))

function _validated_stream(
    stream,
    empty_message::AbstractString;
    subject::AbstractString,
    expected_schema_version::AbstractString = DEFAULT_SCHEMA_VERSION,
)
    state = iterate(stream)
    state === nothing && throw(ArgumentError(empty_message))
    first_batch, next_state = state
    require_schema_version(
        _validated_batch_table(first_batch);
        subject = subject,
        expected = expected_schema_version,
    )
    return ValidatedStream(stream, first_batch, next_state)
end

function _schema_metadata_entries(metadata)
    return metadata isa AbstractVector ? metadata : pairs(metadata)
end

function _normalized_schema_metadata_text(value; label::AbstractString)
    text = strip(string(value))
    isempty(text) && throw(ArgumentError("$(label) must not be blank"))
    return text
end

function _normalized_schema_version(schema_version::AbstractString)
    return _normalized_schema_metadata_text(
        schema_version;
        label = "WendaoArrow schema version",
    )
end

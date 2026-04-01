function schema_version_metadata()
    return [("wendao.schema_version" => DEFAULT_SCHEMA_VERSION)]
end

function _response_schema_metadata(table_like)
    merged = Dict{String,String}()
    for (key, value) in pairs(schema_metadata(table_like))
        merged[String(key)] = String(value)
    end
    merged["wendao.schema_version"] = DEFAULT_SCHEMA_VERSION
    return collect(merged)
end

function schema_metadata(table)
    metadata = Arrow.getmetadata(table)
    return metadata === nothing ? Dict{String,String}() : metadata
end

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

function _validated_stream(stream, empty_message::AbstractString; subject::AbstractString)
    state = iterate(stream)
    state === nothing && throw(ArgumentError(empty_message))
    first_batch, next_state = state
    require_schema_version(_validated_batch_table(first_batch); subject = subject)
    return ValidatedStream(stream, first_batch, next_state)
end

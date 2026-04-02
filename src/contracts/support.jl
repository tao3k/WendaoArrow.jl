function _normalize_required_columns(required_columns)
    return [String(column) for column in required_columns]
end

function _enum_values_label(allowed_values)
    return "[" * join(allowed_values, ", ") * "]"
end

function _enum_value_labels(::Type{T}) where {T<:Enum}
    return String[string(instance) for instance in instances(T)]
end

function _normalize_enum_string_specs(
    specs;
    subject::AbstractString = "WendaoArrow contract",
    group_label::AbstractString = "enum string specs",
)
    normalized = Pair{String,Vector{String}}[]
    for spec in specs
        spec isa Pair || throw(
            ArgumentError(
                "$(subject) $(group_label) entries must be Pair(name => allowed_values); got $(repr(spec))::$(typeof(spec))",
            ),
        )
        name = String(first(spec))
        allowed_values = unique(_normalize_required_columns(last(spec)))
        isempty(allowed_values) && throw(
            ArgumentError(
                "$(subject) $(group_label) entry $(name) must declare at least one allowed value",
            ),
        )
        any(isempty, allowed_values) && throw(
            ArgumentError(
                "$(subject) $(group_label) entry $(name) must not declare empty allowed values",
            ),
        )
        push!(normalized, name => allowed_values)
    end
    return normalized
end

function _normalize_enum_type_specs(
    specs;
    subject::AbstractString = "WendaoArrow contract",
    group_label::AbstractString = "enum specs",
)
    normalized = Pair{String,DataType}[]
    for spec in specs
        spec isa Pair || throw(
            ArgumentError(
                "$(subject) $(group_label) entries must be Pair(name => enum_type); got $(repr(spec))::$(typeof(spec))",
            ),
        )
        name = String(first(spec))
        enum_type = last(spec)
        enum_type isa DataType && enum_type <: Enum || throw(
            ArgumentError(
                "$(subject) $(group_label) entry $(name) must declare an Enum type; got $(repr(enum_type))::$(typeof(enum_type))",
            ),
        )
        push!(normalized, name => enum_type)
    end
    return normalized
end

function _column_value_location(column::AbstractString, row_index::Union{Nothing,Integer})
    if isnothing(row_index)
        return "column $(column)"
    end
    return "column $(column) row $(row_index)"
end

function _metadata_entries(metadata)
    return metadata isa AbstractVector ? metadata : pairs(metadata)
end

function _copy_string_metadata(metadata)
    metadata === nothing && return nothing
    return Dict(
        String(first(entry)) => String(last(entry)) for entry in _metadata_entries(metadata)
    )
end

function _merge_string_metadata(metadata_sources...)
    merged = Dict{String,String}()
    for metadata in metadata_sources
        metadata === nothing && continue
        for entry in _metadata_entries(metadata)
            merged[String(first(entry))] = String(last(entry))
        end
    end
    return isempty(merged) ? nothing : merged
end

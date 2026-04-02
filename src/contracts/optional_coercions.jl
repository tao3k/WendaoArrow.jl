function coerce_optional_string(
    value;
    column::AbstractString = "value",
    subject::AbstractString = "WendaoArrow request batch",
    row_index::Union{Nothing,Integer} = nothing,
)
    ismissing(value) && return missing
    location = _column_value_location(column, row_index)

    if value isa AbstractString
        result = String(value)
        isempty(result) && throw(
            ArgumentError(
                "$(subject) $(location) must contain non-empty string values or missing; got empty string",
            ),
        )
        return result
    end

    throw(
        ArgumentError(
            "$(subject) $(location) must contain string values or missing; got $(repr(value))::$(typeof(value))",
        ),
    )
end

function coerce_optional_int64(
    value;
    column::AbstractString = "value",
    subject::AbstractString = "WendaoArrow request batch",
    row_index::Union{Nothing,Integer} = nothing,
)
    ismissing(value) && return missing
    location = _column_value_location(column, row_index)

    if value isa Integer
        result = try
            Int64(value)
        catch
            throw(
                ArgumentError(
                    "$(subject) $(location) must contain Int64 values or missing; got $(repr(value))::$(typeof(value))",
                ),
            )
        end
        return result
    end

    if value isa AbstractString
        text = String(value)
        isempty(text) && throw(
            ArgumentError(
                "$(subject) $(location) must contain Int64 values or missing; got empty string",
            ),
        )
        result = try
            parse(Int64, text)
        catch
            throw(
                ArgumentError(
                    "$(subject) $(location) must contain Int64 values or missing; got $(repr(value))::$(typeof(value))",
                ),
            )
        end
        return result
    end

    throw(
        ArgumentError(
            "$(subject) $(location) must contain Int64 values or missing; got $(repr(value))::$(typeof(value))",
        ),
    )
end

function coerce_optional_bool(
    value;
    column::AbstractString = "value",
    subject::AbstractString = "WendaoArrow request batch",
    row_index::Union{Nothing,Integer} = nothing,
)
    ismissing(value) && return missing
    location = _column_value_location(column, row_index)

    if value isa Bool
        return value
    end

    if value isa AbstractString
        text = String(value)
        isempty(text) && throw(
            ArgumentError(
                "$(subject) $(location) must contain Bool values or missing; got empty string",
            ),
        )
        normalized = lowercase(text)
        normalized == "true" && return true
        normalized == "false" && return false
    end

    throw(
        ArgumentError(
            "$(subject) $(location) must contain Bool values or missing; got $(repr(value))::$(typeof(value))",
        ),
    )
end

function coerce_optional_enum_string(
    value;
    column::AbstractString = "value",
    subject::AbstractString = "WendaoArrow request batch",
    row_index::Union{Nothing,Integer} = nothing,
    allowed_values,
)
    ismissing(value) && return missing
    location = _column_value_location(column, row_index)
    expected = _enum_values_label(_normalize_required_columns(allowed_values))

    if value isa AbstractString
        result = String(value)
        isempty(result) && throw(
            ArgumentError(
                "$(subject) $(location) must contain one of $(expected) or missing; got empty string",
            ),
        )
        result in allowed_values && return result
    end

    throw(
        ArgumentError(
            "$(subject) $(location) must contain one of $(expected) or missing; got $(repr(value))::$(typeof(value))",
        ),
    )
end

function coerce_optional_enum(
    value;
    column::AbstractString = "value",
    subject::AbstractString = "WendaoArrow request batch",
    row_index::Union{Nothing,Integer} = nothing,
    enum_type::Type{T},
) where {T<:Enum}
    ismissing(value) && return missing
    location = _column_value_location(column, row_index)
    expected = _enum_values_label(_enum_value_labels(T))

    value isa T && return value

    if value isa Integer && !(value isa Bool)
        try
            return T(value)
        catch
        end
    end

    if value isa AbstractString
        text = String(value)
        isempty(text) && throw(
            ArgumentError(
                "$(subject) $(location) must contain one of $(expected) or missing; got empty string",
            ),
        )
        for instance in instances(T)
            text == string(instance) && return instance
        end
    end

    throw(
        ArgumentError(
            "$(subject) $(location) must contain one of $(expected) or missing; got $(repr(value))::$(typeof(value))",
        ),
    )
end

function _normalize_optional_enum_values(
    values,
    row_count::Integer;
    enum_type::Type{T},
    column::AbstractString,
    subject::AbstractString,
) where {T<:Enum}
    normalized_values = Vector{Union{Missing,T}}(undef, row_count)
    for row_index = 1:row_count
        normalized_values[row_index] = coerce_optional_enum(
            values[row_index];
            column = column,
            subject = subject,
            row_index = row_index,
            enum_type = T,
        )
    end
    return normalized_values
end

function coerce_optional_datetime(
    value;
    column::AbstractString = "value",
    subject::AbstractString = "WendaoArrow request batch",
    row_index::Union{Nothing,Integer} = nothing,
)
    ismissing(value) && return missing
    location = _column_value_location(column, row_index)

    value isa DateTime && return value

    if value isa AbstractString
        text = String(value)
        isempty(text) && throw(
            ArgumentError(
                "$(subject) $(location) must contain ISO8601 DateTime values or missing; got empty string",
            ),
        )
        result = try
            DateTime(text)
        catch
            throw(
                ArgumentError(
                    "$(subject) $(location) must contain ISO8601 DateTime values or missing; got $(repr(value))::$(typeof(value))",
                ),
            )
        end
        return result
    end

    throw(
        ArgumentError(
            "$(subject) $(location) must contain ISO8601 DateTime values or missing; got $(repr(value))::$(typeof(value))",
        ),
    )
end

function coerce_optional_float64(
    value;
    column::AbstractString = "value",
    subject::AbstractString = "WendaoArrow request batch",
    row_index::Union{Nothing,Integer} = nothing,
)
    ismissing(value) && return missing
    location = _column_value_location(column, row_index)

    if value isa Real && !(value isa Bool)
        result = try
            Float64(value)
        catch
            throw(
                ArgumentError(
                    "$(subject) $(location) must contain finite Float64 values or missing; got $(repr(value))::$(typeof(value))",
                ),
            )
        end
        isfinite(result) && return result
        throw(
            ArgumentError(
                "$(subject) $(location) must contain finite Float64 values or missing; got $(repr(result))::$(typeof(result))",
            ),
        )
    end

    if value isa AbstractString
        text = String(value)
        isempty(text) && throw(
            ArgumentError(
                "$(subject) $(location) must contain finite Float64 values or missing; got empty string",
            ),
        )
        result = try
            parse(Float64, text)
        catch
            throw(
                ArgumentError(
                    "$(subject) $(location) must contain finite Float64 values or missing; got $(repr(value))::$(typeof(value))",
                ),
            )
        end
        isfinite(result) && return result
        throw(
            ArgumentError(
                "$(subject) $(location) must contain finite Float64 values or missing; got $(repr(result))::$(typeof(result))",
            ),
        )
    end

    throw(
        ArgumentError(
            "$(subject) $(location) must contain finite Float64 values or missing; got $(repr(value))::$(typeof(value))",
        ),
    )
end

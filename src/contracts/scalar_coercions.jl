function coerce_float64(
    value;
    column::AbstractString = "value",
    subject::AbstractString = "WendaoArrow request batch",
    row_index::Union{Nothing,Integer} = nothing,
)
    location = _column_value_location(column, row_index)
    if ismissing(value)
        throw(
            ArgumentError(
                "$(subject) $(location) must contain numeric values; got missing",
            ),
        )
    end

    result = try
        Float64(value)
    catch
        throw(
            ArgumentError(
                "$(subject) $(location) must contain numeric values; got $(repr(value))::$(typeof(value))",
            ),
        )
    end

    if !isfinite(result)
        throw(
            ArgumentError(
                "$(subject) $(location) must contain finite numeric values; got $(repr(result))::$(typeof(result))",
            ),
        )
    end
    return result
end

function coerce_string(
    value;
    column::AbstractString = "value",
    subject::AbstractString = "WendaoArrow request batch",
    row_index::Union{Nothing,Integer} = nothing,
)
    location = _column_value_location(column, row_index)
    if ismissing(value)
        throw(
            ArgumentError("$(subject) $(location) must contain string values; got missing"),
        )
    end

    if value isa AbstractString
        result = String(value)
        isempty(result) && throw(
            ArgumentError(
                "$(subject) $(location) must contain non-empty string values; got empty string",
            ),
        )
        return result
    end

    throw(
        ArgumentError(
            "$(subject) $(location) must contain string values; got $(repr(value))::$(typeof(value))",
        ),
    )
end

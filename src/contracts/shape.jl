function require_columns(
    table_like,
    required_columns;
    subject::AbstractString = "WendaoArrow request batch",
)
    schema = Tables.schema(table_like)
    available = Set(String(name) for name in schema.names)
    required = _normalize_required_columns(required_columns)
    missing = [column for column in required if column ∉ available]
    isempty(missing) && return nothing

    throw(
        ArgumentError(
            "$(subject) requires columns: $(join(required, ", ")); missing: $(join(missing, ", "))",
        ),
    )
end

function require_schema_version(
    table_like;
    subject::AbstractString = "WendaoArrow request",
    expected::AbstractString = DEFAULT_SCHEMA_VERSION,
)
    metadata = schema_metadata(table_like)
    actual = get(metadata, "wendao.schema_version", nothing)
    if isnothing(actual)
        throw(
            ArgumentError(
                "$(subject) requires schema version $(expected); missing wendao.schema_version metadata",
            ),
        )
    end
    actual == expected && return nothing

    throw(ArgumentError("$(subject) requires schema version $(expected); got $(actual)"))
end

function require_column_lengths(
    table_like,
    required_columns;
    subject::AbstractString = "WendaoArrow request batch",
)
    columns = Tables.columntable(table_like)
    required = _normalize_required_columns(required_columns)
    isempty(required) && return 0
    require_columns(columns, required; subject = subject)

    lengths = Pair{String,Int}[]
    for column in required
        values = getproperty(columns, Symbol(column))
        count = try
            length(values)
        catch
            throw(
                ArgumentError(
                    "$(subject) column $(column) must expose length(...); got $(typeof(values))",
                ),
            )
        end
        push!(lengths, column => count)
    end

    expected_length = last(first(lengths))
    all(last(length_info) == expected_length for length_info in lengths) &&
        return expected_length

    throw(
        ArgumentError(
            "$(subject) requires aligned column lengths; got $(join(["$(column)=$(count)" for (column, count) in lengths], ", "))",
        ),
    )
end

function require_unique_string_column(
    table_like,
    column::AbstractString;
    subject::AbstractString = "WendaoArrow request batch",
    seen = nothing,
    row_offset::Integer = 0,
)
    columns = Tables.columntable(table_like)
    require_columns(columns, (column,); subject = subject)
    values = getproperty(columns, Symbol(column))
    seen_rows = isnothing(seen) ? Dict{String,Int}() : seen

    for (row_index, value) in enumerate(values)
        absolute_row_index = row_offset + row_index
        normalized_value = coerce_string(
            value;
            column = column,
            subject = subject,
            row_index = absolute_row_index,
        )
        previous_row_index = get(seen_rows, normalized_value, nothing)
        if !isnothing(previous_row_index)
            throw(
                ArgumentError(
                    "$(subject) column $(column) row $(absolute_row_index) must contain unique non-empty string values; duplicate $(repr(normalized_value)) already seen at row $(previous_row_index)",
                ),
            )
        end
        seen_rows[normalized_value] = absolute_row_index
    end

    return seen_rows
end

function normalize_scoring_response(
    table_like;
    subject::AbstractString = "WendaoArrow scoring response",
    doc_id_column::AbstractString = "doc_id",
    analyzer_score_column::AbstractString = "analyzer_score",
    final_score_column::AbstractString = "final_score",
    optional_string_columns = (),
    optional_int64_columns = (),
    optional_bool_columns = (),
    optional_float64_columns = (),
    optional_datetime_columns = (),
    optional_enum_string_columns = (),
    optional_enum_columns = (),
)
    required = (doc_id_column, analyzer_score_column, final_score_column)
    normalized_optional_string_columns = [
        column for column in _normalize_required_columns(optional_string_columns) if
        column ∉ required
    ]
    normalized_optional_int64_columns = [
        column for column in _normalize_required_columns(optional_int64_columns) if
        column ∉ required && column ∉ normalized_optional_string_columns
    ]
    normalized_optional_bool_columns = [
        column for column in _normalize_required_columns(optional_bool_columns) if
        column ∉ required &&
        column ∉ normalized_optional_string_columns &&
        column ∉ normalized_optional_int64_columns
    ]
    normalized_optional_float64_columns = [
        column for column in _normalize_required_columns(optional_float64_columns) if
        column ∉ required &&
        column ∉ normalized_optional_string_columns &&
        column ∉ normalized_optional_int64_columns &&
        column ∉ normalized_optional_bool_columns
    ]
    normalized_optional_datetime_columns = [
        column for column in _normalize_required_columns(optional_datetime_columns) if
        column ∉ required &&
        column ∉ normalized_optional_string_columns &&
        column ∉ normalized_optional_int64_columns &&
        column ∉ normalized_optional_bool_columns &&
        column ∉ normalized_optional_float64_columns
    ]
    normalized_optional_enum_specs = [
        column_name => allowed_values for
        (column_name, allowed_values) in _normalize_enum_string_specs(
            optional_enum_string_columns;
            subject = subject,
            group_label = "optional enum string columns",
        ) if column_name ∉ required &&
        column_name ∉ normalized_optional_string_columns &&
        column_name ∉ normalized_optional_int64_columns &&
        column_name ∉ normalized_optional_bool_columns &&
        column_name ∉ normalized_optional_float64_columns &&
        column_name ∉ normalized_optional_datetime_columns
    ]
    normalized_optional_enum_columns =
        [first(spec) for spec in normalized_optional_enum_specs]
    normalized_optional_typed_enum_specs = [
        column_name => enum_type for
        (column_name, enum_type) in _normalize_enum_type_specs(
            optional_enum_columns;
            subject = subject,
            group_label = "optional enum columns",
        ) if column_name ∉ required &&
        column_name ∉ normalized_optional_string_columns &&
        column_name ∉ normalized_optional_int64_columns &&
        column_name ∉ normalized_optional_bool_columns &&
        column_name ∉ normalized_optional_float64_columns &&
        column_name ∉ normalized_optional_datetime_columns &&
        column_name ∉ normalized_optional_enum_columns
    ]
    normalized_optional_typed_enum_columns =
        [first(spec) for spec in normalized_optional_typed_enum_specs]
    row_count = require_column_lengths(
        table_like,
        (
            required...,
            normalized_optional_string_columns...,
            normalized_optional_int64_columns...,
            normalized_optional_bool_columns...,
            normalized_optional_float64_columns...,
            normalized_optional_datetime_columns...,
            normalized_optional_enum_columns...,
            normalized_optional_typed_enum_columns...,
        );
        subject = subject,
    )
    columns = Tables.columntable(table_like)
    schema = Tables.schema(table_like)

    normalized_doc_ids = Vector{String}(undef, row_count)
    normalized_analyzer_scores = Vector{Float64}(undef, row_count)
    normalized_final_scores = Vector{Float64}(undef, row_count)
    normalized_optional_strings = Dict{String,Vector{Union{Missing,String}}}()
    normalized_optional_int64s = Dict{String,Vector{Union{Missing,Int64}}}()
    normalized_optional_bools = Dict{String,Vector{Union{Missing,Bool}}}()
    normalized_optional_float64s = Dict{String,Vector{Union{Missing,Float64}}}()
    normalized_optional_datetimes = Dict{String,Vector{Union{Missing,DateTime}}}()
    normalized_optional_enum_strings = Dict{String,Vector{Union{Missing,String}}}()
    normalized_optional_enums = Dict{String,Any}()
    source_column_metadata = Dict{Symbol,Any}()

    doc_id_values = getproperty(columns, Symbol(doc_id_column))
    analyzer_score_values = getproperty(columns, Symbol(analyzer_score_column))
    final_score_values = getproperty(columns, Symbol(final_score_column))

    for row_index = 1:row_count
        normalized_doc_ids[row_index] = coerce_string(
            doc_id_values[row_index];
            column = doc_id_column,
            subject = subject,
            row_index = row_index,
        )
        normalized_analyzer_scores[row_index] = coerce_float64(
            analyzer_score_values[row_index];
            column = analyzer_score_column,
            subject = subject,
            row_index = row_index,
        )
        normalized_final_scores[row_index] = coerce_float64(
            final_score_values[row_index];
            column = final_score_column,
            subject = subject,
            row_index = row_index,
        )
    end

    for column_name in normalized_optional_string_columns
        values = getproperty(columns, Symbol(column_name))
        normalized_values = Vector{Union{Missing,String}}(undef, row_count)
        for row_index = 1:row_count
            normalized_values[row_index] = coerce_optional_string(
                values[row_index];
                column = column_name,
                subject = subject,
                row_index = row_index,
            )
        end
        normalized_optional_strings[column_name] = normalized_values
    end

    for column_name in normalized_optional_int64_columns
        values = getproperty(columns, Symbol(column_name))
        normalized_values = Vector{Union{Missing,Int64}}(undef, row_count)
        for row_index = 1:row_count
            normalized_values[row_index] = coerce_optional_int64(
                values[row_index];
                column = column_name,
                subject = subject,
                row_index = row_index,
            )
        end
        normalized_optional_int64s[column_name] = normalized_values
    end

    for column_name in normalized_optional_bool_columns
        values = getproperty(columns, Symbol(column_name))
        normalized_values = Vector{Union{Missing,Bool}}(undef, row_count)
        for row_index = 1:row_count
            normalized_values[row_index] = coerce_optional_bool(
                values[row_index];
                column = column_name,
                subject = subject,
                row_index = row_index,
            )
        end
        normalized_optional_bools[column_name] = normalized_values
    end

    for column_name in normalized_optional_float64_columns
        values = getproperty(columns, Symbol(column_name))
        normalized_values = Vector{Union{Missing,Float64}}(undef, row_count)
        for row_index = 1:row_count
            normalized_values[row_index] = coerce_optional_float64(
                values[row_index];
                column = column_name,
                subject = subject,
                row_index = row_index,
            )
        end
        normalized_optional_float64s[column_name] = normalized_values
    end

    for column_name in normalized_optional_datetime_columns
        values = getproperty(columns, Symbol(column_name))
        normalized_values = Vector{Union{Missing,DateTime}}(undef, row_count)
        for row_index = 1:row_count
            normalized_values[row_index] = coerce_optional_datetime(
                values[row_index];
                column = column_name,
                subject = subject,
                row_index = row_index,
            )
        end
        normalized_optional_datetimes[column_name] = normalized_values
    end

    for (column_name, allowed_values) in normalized_optional_enum_specs
        values = getproperty(columns, Symbol(column_name))
        normalized_values = Vector{Union{Missing,String}}(undef, row_count)
        for row_index = 1:row_count
            normalized_values[row_index] = coerce_optional_enum_string(
                values[row_index];
                column = column_name,
                subject = subject,
                row_index = row_index,
                allowed_values = allowed_values,
            )
        end
        normalized_optional_enum_strings[column_name] = normalized_values
    end

    for (column_name, enum_type) in normalized_optional_typed_enum_specs
        values = getproperty(columns, Symbol(column_name))
        normalized_optional_enums[column_name] = _normalize_optional_enum_values(
            values,
            row_count;
            enum_type = enum_type,
            column = column_name,
            subject = subject,
        )
    end

    normalized_columns = Pair{Symbol,Any}[]
    for name in schema.names
        symbol_name = Symbol(name)
        string_name = String(name)
        source_column_metadata[symbol_name] =
            _copy_string_metadata(Arrow.getmetadata(getproperty(columns, symbol_name)))
        values = if string_name == doc_id_column
            normalized_doc_ids
        elseif string_name == analyzer_score_column
            normalized_analyzer_scores
        elseif string_name == final_score_column
            normalized_final_scores
        elseif haskey(normalized_optional_strings, string_name)
            normalized_optional_strings[string_name]
        elseif haskey(normalized_optional_int64s, string_name)
            normalized_optional_int64s[string_name]
        elseif haskey(normalized_optional_bools, string_name)
            normalized_optional_bools[string_name]
        elseif haskey(normalized_optional_float64s, string_name)
            normalized_optional_float64s[string_name]
        elseif haskey(normalized_optional_datetimes, string_name)
            normalized_optional_datetimes[string_name]
        elseif haskey(normalized_optional_enum_strings, string_name)
            normalized_optional_enum_strings[string_name]
        elseif haskey(normalized_optional_enums, string_name)
            normalized_optional_enums[string_name]
        else
            collect(getproperty(columns, symbol_name))
        end
        push!(normalized_columns, symbol_name => values)
    end

    return Arrow.withmetadata(
        (; normalized_columns...);
        metadata = _copy_string_metadata(Arrow.getmetadata(table_like)),
        colmetadata = source_column_metadata,
    )
end

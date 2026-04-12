const PARSER_SUMMARY_LIKE_ROW_COUNT = 1075

function build_parser_summary_like_example_processor(;
    request_subject,
    row_count::Integer = PARSER_SUMMARY_LIKE_ROW_COUNT,
)
    return function processor(stream)
        seen_doc_ids = Dict{String,Int}()
        row_offset = 0

        for batch in stream
            normalized_rows = normalize_stream_request_rows(
                batch;
                subject = request_subject,
                seen_doc_ids = seen_doc_ids,
                row_offset = row_offset,
            )
            row_offset += length(normalized_rows)
        end

        return _parser_summary_like_response(row_count)
    end
end

function build_parser_summary_request_example_processor(;
    request_subject,
    row_count::Integer = PARSER_SUMMARY_LIKE_ROW_COUNT,
)
    return function processor(stream)
        request_ids = String[]
        source_ids = String[]
        source_bytes = 0

        for batch in stream
            normalized_requests =
                _normalize_parser_summary_requests(batch; subject = request_subject)
            append!(request_ids, first.(normalized_requests))
            append!(source_ids, getindex.(normalized_requests, 2))
            source_bytes += sum(length(last(request)) for request in normalized_requests)
        end

        isempty(request_ids) &&
            throw(ArgumentError("$(request_subject) must contain at least one request row"))

        response = _parser_summary_like_response(
            row_count;
            request_id = request_ids[1],
            subject_path = source_ids[1],
        )
        return Arrow.withmetadata(
            response;
            metadata = Dict(
                pairs(Arrow.getmetadata(response))...,
                "wendao.request_source_bytes" => string(source_bytes),
            ),
        )
    end
end

function _normalize_parser_summary_requests(batch; subject::AbstractString)
    required_columns = ("request_id", "source_id", "source_text")
    WendaoArrow.require_columns(batch, required_columns; subject = subject)
    row_count =
        WendaoArrow.require_column_lengths(batch, required_columns; subject = subject)
    columns = Tables.columntable(batch)
    return [
        (
            WendaoArrow.coerce_string(
                columns.request_id[row_index];
                column = "request_id",
                subject = subject,
                row_index = row_index,
            ),
            WendaoArrow.coerce_string(
                columns.source_id[row_index];
                column = "source_id",
                subject = subject,
                row_index = row_index,
            ),
            WendaoArrow.coerce_string(
                columns.source_text[row_index];
                column = "source_text",
                subject = subject,
                row_index = row_index,
            ),
        ) for row_index = 1:row_count
    ]
end

function _parser_summary_like_response(
    row_count::Integer;
    request_id::AbstractString = "parser-summary-like",
    subject_path::AbstractString = "Modelica/Blocks/package.mo",
)
    row_count > 0 || throw(ArgumentError("parser summary row_count must be positive"))

    request_id_column = fill(String(request_id), row_count)
    success = fill(true, row_count)
    backend = fill("OMParser.jl", row_count)
    module_name = fill("Blocks", row_count)
    module_kind = fill("package", row_count)
    class_name = fill("Blocks", row_count)
    restriction = fill("package", row_count)

    item_group = String[]
    item_kind = Union{String,Missing}[]
    item_name = String[]
    item_path = String[]
    item_signature = Union{String,Missing}[]
    item_content = Union{String,Missing}[]
    item_line_start = Int64[]
    item_line_end = Int64[]
    item_target_line_start = Union{Int64,Missing}[]
    item_target_line_end = Union{Int64,Missing}[]
    item_owner_kind = Union{String,Missing}[]
    item_owner_path = Union{String,Missing}[]
    item_module_name = Union{String,Missing}[]
    item_module_path = Union{String,Missing}[]
    item_target_path = Union{String,Missing}[]
    item_class_path = Union{String,Missing}[]
    item_visibility = Union{String,Missing}[]
    item_dependency_kind = Union{String,Missing}[]
    item_dependency_form = Union{String,Missing}[]
    item_dependency_target = Union{String,Missing}[]
    item_dependency_local_name = Union{String,Missing}[]
    item_dependency_alias = Union{String,Missing}[]
    item_top_level = Union{Bool,Missing}[]
    item_modifier_names = Union{String,Missing}[]
    item_array_dimensions = Union{String,Missing}[]
    item_start_value = Union{String,Missing}[]
    item_variability = Union{String,Missing}[]
    item_type_name = Union{String,Missing}[]

    for row_index = 1:row_count
        selector = mod1(row_index, 6)
        line_start = row_index
        line_end = row_index + 1
        target_line_start = row_index + 2
        target_line_end = row_index + 3

        if selector == 1
            push!(item_group, "symbol")
            push!(item_kind, "model")
            push!(item_name, "RealInput$(row_index)")
            push!(item_path, "Modelica.Blocks.Interfaces.RealInput$(row_index)")
            push!(item_signature, "connector RealInput$(row_index)")
            push!(item_content, missing)
            push!(item_owner_kind, "package")
            push!(item_owner_path, "Modelica.Blocks.Interfaces")
            push!(item_module_name, "Interfaces")
            push!(item_module_path, "Modelica.Blocks.Interfaces")
            push!(item_target_path, "Modelica.Blocks.Interfaces.RealInput$(row_index)")
            push!(item_class_path, "Modelica.Blocks.Interfaces.RealInput$(row_index)")
            push!(item_visibility, "public")
            push!(item_dependency_kind, missing)
            push!(item_dependency_form, missing)
            push!(item_dependency_target, missing)
            push!(item_dependency_local_name, missing)
            push!(item_dependency_alias, missing)
            push!(item_top_level, true)
            push!(item_modifier_names, "unit,start")
            push!(item_array_dimensions, "[1]")
            push!(item_start_value, "0")
            push!(item_variability, "continuous")
            push!(item_type_name, "Real")
        elseif selector == 2
            push!(item_group, "documentation")
            push!(item_kind, "docstring")
            push!(item_name, "doc$(row_index)")
            push!(item_path, "Modelica.Blocks.Doc$(row_index)")
            push!(item_signature, missing)
            push!(
                item_content,
                repeat("Modelica Blocks parser summary excerpt $(row_index). ", 8),
            )
            push!(item_owner_kind, "package")
            push!(item_owner_path, "Modelica.Blocks")
            push!(item_module_name, "Blocks")
            push!(item_module_path, "Modelica.Blocks")
            push!(item_target_path, "Modelica.Blocks.Interfaces.RealInput$(row_index - 1)")
            push!(item_class_path, "Modelica.Blocks")
            push!(item_visibility, "public")
            push!(item_dependency_kind, missing)
            push!(item_dependency_form, missing)
            push!(item_dependency_target, missing)
            push!(item_dependency_local_name, missing)
            push!(item_dependency_alias, missing)
            push!(item_top_level, true)
            push!(item_modifier_names, missing)
            push!(item_array_dimensions, missing)
            push!(item_start_value, missing)
            push!(item_variability, missing)
            push!(item_type_name, missing)
        elseif selector == 3
            push!(item_group, "import")
            push!(item_kind, "import")
            push!(item_name, "Modelica.SIunits")
            push!(item_path, "Modelica.Blocks.package")
            push!(item_signature, missing)
            push!(item_content, missing)
            push!(item_owner_kind, "package")
            push!(item_owner_path, "Modelica.Blocks")
            push!(item_module_name, "Blocks")
            push!(item_module_path, "Modelica.Blocks")
            push!(item_target_path, "Modelica.Units.SI")
            push!(item_class_path, "Modelica.Blocks")
            push!(item_visibility, "public")
            push!(item_dependency_kind, "import")
            push!(item_dependency_form, "named_import")
            push!(item_dependency_target, "Modelica.Units.SI")
            push!(item_dependency_local_name, "SI")
            push!(item_dependency_alias, "SI")
            push!(item_top_level, true)
            push!(item_modifier_names, missing)
            push!(item_array_dimensions, missing)
            push!(item_start_value, missing)
            push!(item_variability, missing)
            push!(item_type_name, missing)
        elseif selector == 4
            push!(item_group, "extend")
            push!(item_kind, "extends")
            push!(item_name, "Icons")
            push!(item_path, "Modelica.Blocks.Icons")
            push!(item_signature, missing)
            push!(item_content, missing)
            push!(item_owner_kind, "package")
            push!(item_owner_path, "Modelica.Blocks")
            push!(item_module_name, "Blocks")
            push!(item_module_path, "Modelica.Blocks")
            push!(item_target_path, "Modelica.Icons")
            push!(item_class_path, "Modelica.Icons")
            push!(item_visibility, "public")
            push!(item_dependency_kind, "extends")
            push!(item_dependency_form, "extends")
            push!(item_dependency_target, "Modelica.Icons")
            push!(item_dependency_local_name, missing)
            push!(item_dependency_alias, missing)
            push!(item_top_level, true)
            push!(item_modifier_names, missing)
            push!(item_array_dimensions, missing)
            push!(item_start_value, missing)
            push!(item_variability, missing)
            push!(item_type_name, missing)
        elseif selector == 5
            push!(item_group, "parameter")
            push!(item_kind, "parameter")
            push!(item_name, "gain$(row_index)")
            push!(item_path, "Modelica.Blocks.Math.Gain.gain$(row_index)")
            push!(item_signature, "parameter Real gain$(row_index)")
            push!(item_content, missing)
            push!(item_owner_kind, "model")
            push!(item_owner_path, "Modelica.Blocks.Math.Gain")
            push!(item_module_name, "Gain")
            push!(item_module_path, "Modelica.Blocks.Math.Gain")
            push!(item_target_path, "Modelica.Blocks.Math.Gain.gain$(row_index)")
            push!(item_class_path, "Modelica.Blocks.Math.Gain")
            push!(item_visibility, "public")
            push!(item_dependency_kind, missing)
            push!(item_dependency_form, missing)
            push!(item_dependency_target, missing)
            push!(item_dependency_local_name, missing)
            push!(item_dependency_alias, missing)
            push!(item_top_level, false)
            push!(item_modifier_names, "unit,start")
            push!(item_array_dimensions, missing)
            push!(item_start_value, "1.0")
            push!(item_variability, "parameter")
            push!(item_type_name, "Real")
        else
            push!(item_group, "equation")
            push!(item_kind, "equation")
            push!(item_name, "eq$(row_index)")
            push!(item_path, "Modelica.Blocks.Math.Gain#eq$(row_index)")
            push!(item_signature, missing)
            push!(
                item_content,
                "y = gain$(row_index - 1) * u; annotation(Line(points={{0,0},{1,1}}));",
            )
            push!(item_owner_kind, "model")
            push!(item_owner_path, "Modelica.Blocks.Math.Gain")
            push!(item_module_name, "Gain")
            push!(item_module_path, "Modelica.Blocks.Math.Gain")
            push!(item_target_path, "Modelica.Blocks.Math.Gain#eq$(row_index)")
            push!(item_class_path, "Modelica.Blocks.Math.Gain")
            push!(item_visibility, "protected")
            push!(item_dependency_kind, missing)
            push!(item_dependency_form, missing)
            push!(item_dependency_target, missing)
            push!(item_dependency_local_name, missing)
            push!(item_dependency_alias, missing)
            push!(item_top_level, false)
            push!(item_modifier_names, missing)
            push!(item_array_dimensions, missing)
            push!(item_start_value, missing)
            push!(item_variability, missing)
            push!(item_type_name, missing)
        end

        push!(item_line_start, line_start)
        push!(item_line_end, line_end)
        push!(item_target_line_start, target_line_start)
        push!(item_target_line_end, target_line_end)
    end

    base_columns = (
        request_id = request_id_column,
        success = success,
        backend = backend,
        module_name = module_name,
        module_kind = module_kind,
        class_name = class_name,
        restriction = restriction,
        item_group = item_group,
        item_kind = item_kind,
        item_name = item_name,
        item_path = item_path,
        item_signature = item_signature,
        item_content = item_content,
        item_line_start = item_line_start,
        item_line_end = item_line_end,
        item_target_line_start = item_target_line_start,
        item_target_line_end = item_target_line_end,
        item_owner_kind = item_owner_kind,
        item_owner_path = item_owner_path,
        item_module_name = item_module_name,
        item_module_path = item_module_path,
        item_target_path = item_target_path,
        item_class_path = item_class_path,
        item_visibility = item_visibility,
        item_dependency_kind = item_dependency_kind,
        item_dependency_form = item_dependency_form,
        item_dependency_target = item_dependency_target,
        item_dependency_local_name = item_dependency_local_name,
        item_dependency_alias = item_dependency_alias,
        item_top_level = item_top_level,
        item_modifier_names = item_modifier_names,
        item_array_dimensions = item_array_dimensions,
        item_start_value = item_start_value,
        item_variability = item_variability,
        item_type_name = item_type_name,
    )

    filler_columns = [
        if mod(column_index, 3) == 1
            Symbol("item_parser_attr_$(lpad(column_index, 2, '0'))") =>
                Union{String,Missing}[
                    iszero(mod(row_index + column_index, 5)) ? missing :
                    "attr$(column_index)-$(row_index)" for row_index = 1:row_count
                ]
        elseif mod(column_index, 3) == 2
            Symbol("item_parser_attr_$(lpad(column_index, 2, '0'))") =>
                Union{Int64,Missing}[
                    iszero(mod(row_index + column_index, 7)) ? missing :
                    Int64(row_index + column_index) for row_index = 1:row_count
                ]
        else
            Symbol("item_parser_attr_$(lpad(column_index, 2, '0'))") =>
                Union{Bool,Missing}[
                    iszero(mod(row_index + column_index, 11)) ? missing :
                    iszero(mod(row_index + column_index, 2)) for row_index = 1:row_count
                ]
        end for column_index = 1:24
    ]

    response_columns = (; base_columns..., NamedTuple(filler_columns)...)
    return WendaoArrow.schema_table(
        response_columns;
        metadata = [
            "wendao.schema_version" => WendaoArrow.DEFAULT_SCHEMA_VERSION,
            "wendao.response_shape" => "parser-summary-like",
            "wendao.row_count" => string(row_count),
            "wendao.subject_path" => subject_path,
        ],
    )
end

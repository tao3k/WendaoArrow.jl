const LIST_ROUNDTRIP_REQUEST_COLUMNS =
    ("request_id", "anchor_values", "edge_kinds", "candidate_node_ids")

function build_list_roundtrip_example_processor(;
    request_subject,
    response_metadata = nothing,
)
    return function processor(table)
        WendaoArrow.require_columns(
            table,
            LIST_ROUNDTRIP_REQUEST_COLUMNS;
            subject = request_subject,
        )
        row_count = WendaoArrow.require_column_lengths(
            table,
            LIST_ROUNDTRIP_REQUEST_COLUMNS;
            subject = request_subject,
        )

        columns = Tables.columntable(table)
        request_ids = String[]
        echoed_anchor_values = Vector{String}[]
        echoed_edge_kinds = Vector{String}[]
        pin_assignment = Vector{String}[]
        candidate_size = Int64[]

        for row_index = 1:row_count
            request_id = WendaoArrow.coerce_string(
                columns.request_id[row_index];
                column = "request_id",
                subject = request_subject,
                row_index = row_index,
            )
            anchor_values = _list_roundtrip_strings(
                columns.anchor_values[row_index];
                column = "anchor_values",
                subject = request_subject,
                row_index = row_index,
            )
            edge_kinds = _list_roundtrip_strings(
                columns.edge_kinds[row_index];
                column = "edge_kinds",
                subject = request_subject,
                row_index = row_index,
            )
            candidate_node_ids = _list_roundtrip_strings(
                columns.candidate_node_ids[row_index];
                column = "candidate_node_ids",
                subject = request_subject,
                row_index = row_index,
            )

            push!(request_ids, request_id)
            push!(echoed_anchor_values, anchor_values)
            push!(echoed_edge_kinds, edge_kinds)
            push!(
                pin_assignment,
                isempty(candidate_node_ids) ? String[] : String[candidate_node_ids[1]],
            )
            push!(candidate_size, Int64(length(candidate_node_ids)))
        end

        return WendaoArrow.schema_table(
            (
                request_id = request_ids,
                echoed_anchor_values = echoed_anchor_values,
                echoed_edge_kinds = echoed_edge_kinds,
                pin_assignment = pin_assignment,
                candidate_size = candidate_size,
            );
            metadata = response_metadata,
        )
    end
end

function _list_roundtrip_strings(
    value;
    column::AbstractString,
    subject::AbstractString,
    row_index::Integer,
)
    value isa AbstractVector || throw(
        ArgumentError(
            "$(subject) $(column) row $(row_index) must be a vector of strings; got $(repr(value))::$(typeof(value))",
        ),
    )
    normalized = String[]
    for entry in value
        push!(
            normalized,
            WendaoArrow.coerce_string(
                entry;
                column = column,
                subject = subject,
                row_index = row_index,
            ),
        )
    end
    return normalized
end

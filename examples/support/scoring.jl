function build_stream_scoring_example_processor(;
    request_subject,
    response_subject,
    response_mutator = identity,
)
    return function processor(stream)
        doc_ids = String[]
        analyzer_scores = Float64[]
        final_scores = Float64[]
        seen_doc_ids = Dict{String,Int}()
        row_offset = 0

        for batch in stream
            normalized_rows = normalize_stream_request_rows(
                batch;
                subject = request_subject,
                seen_doc_ids = seen_doc_ids,
                row_offset = row_offset,
            )
            for (normalized_doc_id, score) in normalized_rows
                push!(doc_ids, normalized_doc_id)
                push!(analyzer_scores, score)
                push!(final_scores, score)
            end
            row_offset += length(normalized_rows)
        end

        response = response_mutator((
            doc_id = doc_ids,
            analyzer_score = analyzer_scores,
            final_score = final_scores,
        ))

        return WendaoArrow.normalize_scoring_response(response; subject = response_subject)
    end
end

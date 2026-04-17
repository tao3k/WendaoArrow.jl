using WendaoArrow

const Arrow = WendaoArrow.Arrow

include("support.jl")

using .WendaoArrowExampleSupport

function response_table_with_schema_metadata(table_like)
    io = IOBuffer()
    Arrow.write(
        io,
        table_like;
        metadata = [
            "wendao.schema_version" => "shadowed",
            "analyzer.name" => "flight-schema-metadata-demo",
            "response.mode" => "passthrough",
        ],
        colmetadata = Dict(
            :analyzer_score => ["semantic.role" => "analyzer-score"],
            :final_score => ["semantic.role" => "final-score"],
        ),
    )
    return Arrow.Table(IOBuffer(take!(io)); convert = true)
end

processor = let
    request_subject = "WendaoArrow schema metadata Flight request batch"
    response_subject = "WendaoArrow schema metadata Flight response"
    function (stream)
        doc_ids = String[]
        analyzer_scores = Float64[]
        final_scores = Float64[]
        seen_doc_ids = Dict{String,Int}()
        row_offset = 0

        for batch in stream
            normalized_rows = WendaoArrowExampleSupport.normalize_stream_request_rows(
                batch;
                subject = request_subject,
                seen_doc_ids = seen_doc_ids,
                row_offset = row_offset,
            )
            for (doc_id, score) in normalized_rows
                push!(doc_ids, doc_id)
                push!(analyzer_scores, score)
                push!(final_scores, score)
            end
            row_offset += length(normalized_rows)
        end

        source = response_table_with_schema_metadata((
            doc_id = doc_ids,
            analyzer_score = analyzer_scores,
            final_score = final_scores,
        ))
        return WendaoArrow.normalize_scoring_response(source; subject = response_subject)
    end
end

config = WendaoArrow.config_from_args(ARGS)

WendaoArrow.serve_stream_flight(processor; host = config.host, port = config.port)

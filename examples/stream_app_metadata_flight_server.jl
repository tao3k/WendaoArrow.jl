using WendaoArrow
using Tables
using gRPCServer

const Arrow = WendaoArrow.Arrow

config = WendaoArrow.config_from_args(ARGS)

processor = function (table)
    columns = Tables.columntable(table)
    partitions = Tables.partitioner(
        Tuple(
            (
                doc_id = [String(doc_id)],
                analyzer_score = [Float64(vector_score)],
                final_score = [Float64(vector_score)],
            ) for
            (doc_id, vector_score) in zip(columns.doc_id, columns.vector_score)
        ),
    )
    return Arrow.Flight.withappmetadata(
        partitions;
        app_metadata = ["score-batch:$(index - 1)" for index in eachindex(columns.doc_id)],
    )
end

WendaoArrow.serve_flight(processor; host = config.host, port = config.port)

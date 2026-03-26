function decode_ipc(body)::Arrow.Table
    table = Arrow.Table(IOBuffer(body))
    schema = Tables.schema(table)
    if isempty(schema.names)
        throw(ArgumentError("Arrow IPC request must contain at least one column"))
    end
    return table
end

function encode_ipc(table_like)::Vector{UInt8}
    io = IOBuffer()
    Arrow.write(io, table_like)
    return take!(io)
end

@testset "cross-process Flight startup script serves pyarrow DoExchange for list columns" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_list_roundtrip_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(pyarrow_list_doexchange_command(python, port))
        @test chomp(output) ==
              "{\"candidate_size\": [2, 1], \"echoed_anchor_values\": [[\"graph\", \"retrieval\"], [\"shared-tag\"]], \"echoed_edge_kinds\": [[\"depends_on\", \"semantic_similar\"], [\"references\"]], \"pin_assignment\": [[\"node-a\"], [\"node-c\"]], \"request_id\": [\"request-a\", \"request-b\"]}"
    end
end

@testset "cross-process Flight startup script serves native Julia DoExchange for list columns" begin
    expected = expected_list_roundtrip_response()
    with_list_roundtrip_flight_server() do port, process
        @test Base.process_running(process)
        response_table = native_julia_list_doexchange_table(port)
        assert_list_roundtrip_columns(response_table, expected)
    end
end

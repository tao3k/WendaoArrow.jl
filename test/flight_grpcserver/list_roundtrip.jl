@testset "cross-process list Flight responses preserve WendaoArrow list columns" begin
    expected = expected_list_roundtrip_response()
    with_list_roundtrip_flight_server() do port, process
        @test Base.process_running(process)
        response_table = native_julia_list_doexchange_table(port)
        assert_list_roundtrip_columns(response_table, expected)
    end
end

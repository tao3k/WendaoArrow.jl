@testset "cross-process Flight startup script serves native Julia DoExchange for routed list columns" begin
    expected = expected_list_roundtrip_response()
    descriptor = WendaoArrow.flight_route_descriptor("/graph/structural/rerank")
    with_list_route_roundtrip_flight_server() do port, process
        @test Base.process_running(process)
        response_table = native_julia_doexchange_table(
            port;
            source = list_roundtrip_sample_table(),
            descriptor = descriptor,
        )
        assert_list_roundtrip_columns(
            response_table,
            expected;
            response_mode = "route-probe",
        )
    end
end

@testset "cross-process Flight startup script serves native Julia DoExchange for routed list columns with search-like headers" begin
    expected = expected_list_roundtrip_response()
    descriptor = WendaoArrow.flight_route_descriptor("/graph/structural/rerank")
    with_list_route_roundtrip_flight_server() do port, process
        @test Base.process_running(process)
        response_table = native_julia_doexchange_table(
            port;
            source = list_roundtrip_sample_table(),
            descriptor = descriptor,
            headers = search_like_structural_route_headers(),
        )
        assert_list_roundtrip_columns(
            response_table,
            expected;
            response_mode = "route-probe",
        )
    end
end

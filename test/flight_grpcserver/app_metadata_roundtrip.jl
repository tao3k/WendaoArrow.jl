@testset "WendaoArrow product helper preserves response app metadata" begin
    with_app_metadata_flight_server() do port, process
        @test Base.process_running(process)
        response = product_helper_doexchange_table(port; include_app_metadata = true)
        response_table = response.table
        response_columns = Tables.columntable(response_table)

        assert_scoring_columns(response_columns, ["doc-a", "doc-b"], [0.9, 0.5], [0.9, 0.5])
        @test String.(response.app_metadata) == ["score-batch:0", "score-batch:1"]
    end
end

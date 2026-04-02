@testset "cross-process Flight startup script serves pyarrow DoExchange with response app metadata" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_app_metadata_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(pyarrow_app_metadata_doexchange_command(python, port))
        @test chomp(output) ==
              "{\"analyzer_score\": [0.9, 0.5], \"app_metadata\": [\"score-batch:0\", \"score-batch:1\"], \"doc_id\": [\"doc-a\", \"doc-b\"], \"final_score\": [0.9, 0.5]}"
    end
end

@testset "cross-process Flight startup script serves native Julia DoExchange with response app metadata" begin
    with_app_metadata_flight_server() do port, process
        @test Base.process_running(process)
        response = native_julia_doexchange_table(port; include_app_metadata = true)
        response_table = response.table
        response_columns = Tables.columntable(response_table)

        assert_scoring_columns(response_columns, ["doc-a", "doc-b"], [0.9, 0.5], [0.9, 0.5])
        @test String.(response.app_metadata) == ["score-batch:0", "score-batch:1"]
    end
end

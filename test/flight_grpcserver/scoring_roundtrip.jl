@testset "cross-process Flight startup script serves pyarrow DoExchange" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(pyarrow_doexchange_command(python, port))
        @test chomp(output) ==
              "{\"analyzer_score\": [0.9, 0.5], \"doc_id\": [\"doc-a\", \"doc-b\"], \"final_score\": [0.9, 0.5]}"
    end
end

@testset "cross-process Flight startup script serves native Julia DoExchange" begin
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        response = native_julia_doexchange(port)
        assert_scoring_columns(response, ["doc-a", "doc-b"], [0.9, 0.5], [0.9, 0.5])
    end
end

@testset "cross-process Flight startup script serves pyarrow DoExchange across multiple batches" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_doexchange_command(python, port; multi_batch = true),
        )
        @test chomp(output) ==
              "{\"analyzer_score\": [0.9, 0.5, 0.25], \"doc_id\": [\"doc-a\", \"doc-b\", \"doc-c\"], \"final_score\": [0.9, 0.5, 0.25]}"
    end
end

@testset "cross-process Flight startup script serves native Julia DoExchange across multiple batches" begin
    expected = expected_multi_batch_scores()
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        response = native_julia_doexchange(port; multi_batch = true)
        assert_scoring_columns(
            response,
            expected.doc_id,
            expected.analyzer_score,
            expected.final_score,
        )
    end
end

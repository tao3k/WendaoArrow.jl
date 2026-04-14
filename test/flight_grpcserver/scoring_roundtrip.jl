@testset "WendaoArrow product helper serves cross-process scoring Flight responses" begin
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        response = product_helper_doexchange(port)
        assert_scoring_columns(response, ["doc-a", "doc-b"], [0.9, 0.5], [0.9, 0.5])
    end
end

@testset "WendaoArrow product helper serves partitioned scoring Flight responses" begin
    expected = expected_multi_batch_scores()
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        response = product_helper_doexchange(port; multi_batch = true)
        assert_scoring_columns(
            response,
            expected.doc_id,
            expected.analyzer_score,
            expected.final_score,
        )
    end
end

@testset "WendaoArrow product helper serves large scoring Flight responses" begin
    with_large_response_flight_server() do port, process
        @test Base.process_running(process)
        response = product_helper_doexchange(port)
        @test length(response.doc_id) == 1
        @test length(only(response.doc_id)) ==
              WendaoArrowExampleSupport.LARGE_RESPONSE_DOC_ID_BYTES
        @test response.analyzer_score == [0.9]
        @test response.final_score == [0.9]
    end
end

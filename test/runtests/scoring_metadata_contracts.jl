@testset "Scoring response helper normalizes contract columns" begin
    normalized = WendaoArrow.normalize_scoring_response(
        (
            doc_id = ["doc-a", SubString("doc-b", 1, 5)],
            analyzer_score = [1, 0.5],
            final_score = [1.5f0, 0.75],
            trace_id = ["trace-a", "trace-b"],
        );
        subject = "stream scoring response",
    )
    @test normalized.doc_id == ["doc-a", "doc-b"]
    @test normalized.analyzer_score == [1.0, 0.5]
    @test normalized.final_score == [1.5, 0.75]
    @test normalized.trace_id == ["trace-a", "trace-b"]

    missing_column_err = try
        WendaoArrow.normalize_scoring_response(
            (doc_id = ["doc-a"], analyzer_score = [1.0]);
            subject = "stream scoring response",
        )
        nothing
    catch error
        error
    end
    @test missing_column_err isa ArgumentError
    @test occursin(
        "stream scoring response requires columns: doc_id, analyzer_score, final_score",
        sprint(showerror, missing_column_err),
    )

    finite_err = try
        WendaoArrow.normalize_scoring_response(
            (doc_id = ["doc-a"], analyzer_score = [NaN], final_score = [1.0]);
            subject = "stream scoring response",
        )
        nothing
    catch error
        error
    end
    @test finite_err isa ArgumentError
    @test occursin(
        "stream scoring response column analyzer_score row 1 must contain finite numeric values",
        sprint(showerror, finite_err),
    )
    @test occursin("got NaN::Float64", sprint(showerror, finite_err))
end

@testset "Scoring response helper preserves schema and column metadata" begin
    source = arrow_table_with_metadata(
        (
            doc_id = ["doc-a", "doc-b"],
            analyzer_score = [1, 0.5],
            final_score = [1.5f0, 0.75],
        );
        metadata = [
            "wendao.schema_version" => "shadowed",
            "analyzer.name" => "normalized-response-demo",
        ],
        colmetadata = Dict(
            :analyzer_score => ["semantic.role" => "analyzer-score"],
            :final_score => ["semantic.role" => "final-score"],
        ),
    )
    normalized =
        WendaoArrow.normalize_scoring_response(source; subject = "stream scoring response")

    @test normalized.doc_id == ["doc-a", "doc-b"]
    @test normalized.analyzer_score == [1.0, 0.5]
    @test normalized.final_score == [1.5, 0.75]
    @test WendaoArrow.schema_metadata(normalized)["wendao.schema_version"] == "shadowed"
    @test WendaoArrow.schema_metadata(normalized)["analyzer.name"] ==
          "normalized-response-demo"
    @test column_metadata(normalized, :analyzer_score)["semantic.role"] == "analyzer-score"
    @test column_metadata(normalized, :final_score)["semantic.role"] == "final-score"
end

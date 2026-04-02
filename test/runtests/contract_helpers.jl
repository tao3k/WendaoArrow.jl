@testset "Column contract helper rejects missing columns" begin
    err = try
        WendaoArrow.require_columns(
            (wrong_score = [1.0],),
            ("doc_id", "vector_score");
            subject = "stream scoring batch",
        )
        nothing
    catch error
        error
    end
    @test err isa ArgumentError
    @test occursin(
        "stream scoring batch requires columns: doc_id, vector_score",
        sprint(showerror, err),
    )
    @test occursin("missing: doc_id, vector_score", sprint(showerror, err))
end

@testset "Column length helper rejects mismatched columns" begin
    @test WendaoArrow.require_column_lengths(
        sample_table(),
        ("doc_id", "vector_score");
        subject = "stream scoring batch",
    ) == 2

    err = try
        WendaoArrow.require_column_lengths(
            (doc_id = ["doc-a", "doc-b"], vector_score = [0.9]),
            ("doc_id", "vector_score");
            subject = "stream scoring batch",
        )
        nothing
    catch error
        error
    end
    @test err isa ArgumentError
    @test occursin(
        "stream scoring batch requires aligned column lengths",
        sprint(showerror, err),
    )
    @test occursin("doc_id=2", sprint(showerror, err))
    @test occursin("vector_score=1", sprint(showerror, err))
end

@testset "Unique string column helper rejects duplicate doc_id values" begin
    seen_rows = WendaoArrow.require_unique_string_column(
        sample_table(),
        "doc_id";
        subject = "stream scoring batch",
    )
    @test seen_rows == Dict("doc-a" => 1, "doc-b" => 2)

    duplicate_err = try
        WendaoArrow.require_unique_string_column(
            (doc_id = ["doc-a", "doc-b", "doc-a"],),
            "doc_id";
            subject = "stream scoring batch",
        )
        nothing
    catch error
        error
    end
    @test duplicate_err isa ArgumentError
    @test occursin(
        "stream scoring batch column doc_id row 3 must contain unique non-empty string values",
        sprint(showerror, duplicate_err),
    )
    @test occursin(
        "duplicate \"doc-a\" already seen at row 1",
        sprint(showerror, duplicate_err),
    )

    stream_err = try
        WendaoArrow.require_unique_string_column(
            (doc_id = ["doc-c", "doc-a"],),
            "doc_id";
            subject = "stream scoring batch",
            seen = seen_rows,
            row_offset = 2,
        )
        nothing
    catch error
        error
    end
    @test stream_err isa ArgumentError
    @test occursin(
        "stream scoring batch column doc_id row 4 must contain unique non-empty string values",
        sprint(showerror, stream_err),
    )
    @test occursin(
        "duplicate \"doc-a\" already seen at row 1",
        sprint(showerror, stream_err),
    )
end

@testset "Schema version helper rejects missing and mismatched versions" begin
    missing_err = try
        WendaoArrow.require_schema_version(
            Arrow.Table(IOBuffer(arrow_ipc_bytes(sample_table())));
            subject = "sample request",
        )
        nothing
    catch error
        error
    end
    @test missing_err isa ArgumentError
    @test occursin(
        "sample request requires schema version v1",
        sprint(showerror, missing_err),
    )
    @test occursin("missing wendao.schema_version metadata", sprint(showerror, missing_err))

    mismatched_err = try
        WendaoArrow.require_schema_version(
            Arrow.Table(
                IOBuffer(
                    arrow_ipc_bytes(
                        sample_table();
                        metadata = ["wendao.schema_version" => "v999"],
                    ),
                ),
            );
            subject = "sample request",
        )
        nothing
    catch error
        error
    end
    @test mismatched_err isa ArgumentError
    @test occursin(
        "sample request requires schema version v1; got v999",
        sprint(showerror, mismatched_err),
    )
end

@testset "Float64 coercion helper rejects invalid values" begin
    @test WendaoArrow.coerce_float64(
        0.25;
        column = "vector_score",
        subject = "stream scoring batch",
        row_index = 2,
    ) == 0.25

    invalid_err = try
        WendaoArrow.coerce_float64(
            "oops";
            column = "vector_score",
            subject = "stream scoring batch",
            row_index = 2,
        )
        nothing
    catch error
        error
    end
    @test invalid_err isa ArgumentError
    @test occursin(
        "stream scoring batch column vector_score row 2 must contain numeric values",
        sprint(showerror, invalid_err),
    )
    @test occursin("\"oops\"::String", sprint(showerror, invalid_err))
end

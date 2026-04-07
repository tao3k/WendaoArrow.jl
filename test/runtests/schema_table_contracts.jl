@testset "Schema metadata merge helper overlays schema version" begin
    merged = Dict(
        WendaoArrow.merge_schema_metadata(
            ["wendao.schema_version" => "shadowed", "source" => "input"],
            Dict("route" => "graph");
            schema_version = "v0-draft",
            subject = "draft schema metadata",
        ),
    )
    @test merged["wendao.schema_version"] == "v0-draft"
    @test merged["source"] == "input"
    @test merged["route"] == "graph"

    blank_err = try
        WendaoArrow.merge_schema_metadata(["route" => "graph"]; schema_version = "")
        nothing
    catch error
        error
    end
    @test blank_err isa ArgumentError
    @test occursin(
        "WendaoArrow schema version must not be blank",
        sprint(showerror, blank_err),
    )
end

@testset "Schema table builder stamps schema metadata on raw tables" begin
    table = WendaoArrow.schema_table(
        sample_table();
        schema_version = "v0-draft",
        metadata = ["route" => "graph-search", "payload_kind" => "request"],
        colmetadata = Dict(:vector_score => ["semantic.role" => "retrieval-score"]),
    )
    WendaoArrow.require_schema_version(
        table;
        subject = "draft schema table",
        expected = "v0-draft",
    )

    metadata = WendaoArrow.schema_metadata(table)
    @test metadata["wendao.schema_version"] == "v0-draft"
    @test metadata["route"] == "graph-search"
    @test metadata["payload_kind"] == "request"
    @test column_metadata(table, :vector_score)["semantic.role"] == "retrieval-score"
end

@testset "Schema table builder preserves source metadata" begin
    source = arrow_table_with_metadata(
        sample_table();
        metadata = ["source" => "input", "wendao.schema_version" => "shadowed"],
    )
    normalized = WendaoArrow.schema_table(source; schema_version = "v0-draft")
    metadata = WendaoArrow.schema_metadata(normalized)
    @test metadata["source"] == "input"
    @test metadata["wendao.schema_version"] == "v0-draft"
end

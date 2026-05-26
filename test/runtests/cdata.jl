@testset "Arrow C Data interface is delegated to upstream Arrow.jl" begin
    capabilities = WendaoArrow.cdata_interface_capabilities()
    @test capabilities.available
    @test capabilities.has_cdata
    @test capabilities.same_process_only
    @test capabilities.header_available
    @test basename(capabilities.header_path) == "arrow_julia_cdata.h"
    @test all(values(capabilities.surfaces))

    required = WendaoArrow.require_cdata_interface()
    @test required.available
    @test required.header_path == capabilities.header_path

    exported = Arrow.CData.exporttable(Arrow.Table(arrow_ipc_bytes(sample_table())))
    try
        imported = Arrow.CData.importtable(
            Arrow.CData.schema_ptr(exported),
            Arrow.CData.array_ptr(exported),
        )
        imported_columns = Tables.columntable(imported)
        @test imported_columns.doc_id == ["doc-a", "doc-b"]
        @test imported_columns.vector_score == [0.9, 0.5]
    finally
        Arrow.CData.release!(exported)
    end
end

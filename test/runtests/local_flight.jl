@testset "Arrow Flight surface available" begin
    @test isdefined(Arrow, :Flight)
    descriptor = WendaoArrow.flight_descriptor()
    @test descriptor.path == collect(WendaoArrow.DEFAULT_FLIGHT_DESCRIPTOR_PATH)
end

@testset "Flight listener backend contract is explicit" begin
    grpcserver = WendaoArrow.flight_listener_backend_capabilities()
    @test grpcserver.backend == :grpcserver
    @test grpcserver.request_streaming
    @test grpcserver.response_streaming
    @test grpcserver.response_trailers
    @test grpcserver.bidirectional_doexchange
    @test isempty(grpcserver.blockers)
    @test WendaoArrow.flight_listener_backend_supported()

    legacy_failure = try
        WendaoArrow.flight_listener_backend_capabilities(:purehttp2)
        nothing
    catch error
        error
    end
    @test legacy_failure isa ArgumentError
    legacy_message = sprint(showerror, legacy_failure)
    @test occursin("backend :purehttp2", legacy_message)
    @test occursin("retired", legacy_message)
    @test occursin(":grpcserver", legacy_message)
    @test occursin("packaged Arrow listener surface", legacy_message)

    nghttp2 = WendaoArrow.flight_listener_backend_capabilities(:nghttp2)
    @test nghttp2.backend == :nghttp2
    @test !nghttp2.request_streaming
    @test !nghttp2.response_streaming
    @test !nghttp2.response_trailers
    @test !nghttp2.bidirectional_doexchange
    @test length(nghttp2.blockers) >= 2
    @test occursin("Nghttp2Wrapper", nghttp2.blockers[1])
    @test occursin("gRPCServer.jl", nghttp2.blockers[2])
    @test !WendaoArrow.flight_listener_backend_supported(:nghttp2)
    @test_throws ArgumentError WendaoArrow.flight_listener_backend_capabilities(:unknown)
end

@testset "Unsupported Flight listener backends fail explicitly" begin
    listener_message = try
        service = WendaoArrow.build_flight_service(identity)
        WendaoArrow.flight_server(service; backend = :nghttp2)
        nothing
    catch error
        @test error isa ArgumentError
        sprint(showerror, error)
    end
    @test !isnothing(listener_message)
    @test occursin("backend :nghttp2", listener_message)
    @test occursin("Nghttp2Wrapper", listener_message)
    @test occursin("gRPCServer.jl", listener_message)

    stream_message = try
        WendaoArrow.serve_stream_flight(identity; backend = :nghttp2)
        nothing
    catch error
        @test error isa ArgumentError
        sprint(showerror, error)
    end
    @test !isnothing(stream_message)
    @test occursin("Nghttp2Wrapper", stream_message)
    @test occursin("gRPCServer.jl", stream_message)
end

@testset "Arrow HTTP/2 listener wrappers start local servers" begin
    service = WendaoArrow.build_flight_service(identity)
    server = WendaoArrow.flight_server(
        service;
        host = WendaoArrow.DEFAULT_HOST,
        port = 0,
        max_active_requests = 2,
        request_capacity = 4,
        response_capacity = 4,
    )

    try
        @test isopen(server)
        @test hasproperty(server, :host)
        @test hasproperty(server, :port)
        @test server.port > 0
        @test getfield(server, :request_gate).max_active_requests == 2
    finally
        Arrow.Flight.stop!(server; force = true)
    end

    unary_server = WendaoArrow.serve_flight(
        identity;
        host = WendaoArrow.DEFAULT_HOST,
        port = 0,
        include_request_app_metadata = true,
        max_active_requests = 2,
        request_capacity = 4,
        response_capacity = 4,
        block = false,
    )

    try
        @test isopen(unary_server)
        @test hasproperty(unary_server, :port)
        @test unary_server.port > 0
        @test getfield(unary_server, :request_gate).max_active_requests == 2
    finally
        Arrow.Flight.stop!(unary_server; force = true)
    end

    streaming_server = WendaoArrow.serve_stream_flight(
        stream -> begin
            doc_ids = String[]
            analyzer_scores = Float64[]
            final_scores = Float64[]

            for batch in stream
                columns = Tables.columntable(batch)
                for (doc_id, vector_score) in zip(columns.doc_id, columns.vector_score)
                    score = Float64(vector_score)
                    push!(doc_ids, doc_id)
                    push!(analyzer_scores, score)
                    push!(final_scores, score + 1.0)
                end
            end

            return (
                doc_id = doc_ids,
                analyzer_score = analyzer_scores,
                final_score = final_scores,
            )
        end;
        host = WendaoArrow.DEFAULT_HOST,
        port = 0,
        include_request_app_metadata = true,
        max_active_requests = 2,
        request_capacity = 4,
        response_capacity = 4,
        block = false,
    )

    try
        @test isopen(streaming_server)
        @test hasproperty(streaming_server, :port)
        @test streaming_server.port > 0
        @test getfield(streaming_server, :request_gate).max_active_requests == 2
    finally
        Arrow.Flight.stop!(streaming_server; force = true)
    end
end

@testset "Flight exchange request wrapper prepares one request" begin
    descriptor = WendaoArrow.flight_route_descriptor("/graph/structural/rerank")
    request = WendaoArrow.flight_exchange_request(
        WendaoArrow.schema_table(sample_table(); schema_version = "v0-draft");
        descriptor = descriptor,
        headers = ["x-trace-id" => "trace-0"],
    )
    @test request.descriptor.path == ["graph", "structural", "rerank"]
    @test request.headers == Pair{String,String}["x-trace-id"=>"trace-0"]

    routed_request = WendaoArrow.flight_exchange_request(
        WendaoArrow.schema_table(sample_table(); schema_version = "v0-draft");
        route = "/graph/structural/filter",
    )
    @test routed_request.descriptor.path == ["graph", "structural", "filter"]

    @test_throws ErrorException WendaoArrow.flight_exchange_request(sample_table())
    @test_throws ErrorException WendaoArrow.flight_exchange_request(
        sample_table();
        descriptor = descriptor,
        route = "/graph/structural/rerank",
    )
end

@testset "Flight exchange request wrapper can invoke local draft service" begin
    descriptor = WendaoArrow.flight_route_descriptor("/graph/structural/rerank")
    service = WendaoArrow.build_flight_service(;
        descriptor = descriptor,
        expected_schema_version = "v0-draft",
    ) do table
        columns = Tables.columntable(table)
        return WendaoArrow.schema_table(
            (
                candidate_id = collect(columns.doc_id),
                analyzer_score = Float64.(columns.vector_score),
                final_score = Float64.(columns.vector_score) .+ 1.0,
            );
            schema_version = "v0-draft",
            metadata = ["response.mode" => "draft-roundtrip"],
        )
    end

    request = WendaoArrow.flight_exchange_request(
        WendaoArrow.schema_table(sample_table(); schema_version = "v0-draft");
        descriptor = descriptor,
        headers = ["x-trace-id" => "trace-1"],
    )
    result_table = WendaoArrow.flight_exchange_table(
        service,
        Arrow.Flight.ServerCallContext(),
        request,
    )
    result = Tables.columntable(result_table)
    metadata = WendaoArrow.schema_metadata(result_table)

    @test result.candidate_id == ["doc-a", "doc-b"]
    @test result.analyzer_score == [0.9, 0.5]
    @test result.final_score == [1.9, 1.5]
    @test metadata["wendao.schema_version"] == "v0-draft"
    @test metadata["response.mode"] == "draft-roundtrip"
end

@testset "Flight table service delegates processing" begin
    descriptor = WendaoArrow.flight_descriptor(["wendao", "arrow", "table"])
    service = WendaoArrow.build_flight_service(; descriptor = descriptor) do table
        columns = Tables.columntable(table)
        return (
            doc_id = collect(columns.doc_id),
            passthrough_score = Float64.(columns.vector_score),
        )
    end

    result_table = Arrow.Flight.table(
        service,
        Arrow.Flight.ServerCallContext(),
        sample_table();
        descriptor = descriptor,
        metadata = VALID_SCHEMA_VERSION_METADATA,
    )
    result = Tables.columntable(result_table)
    metadata = WendaoArrow.schema_metadata(result_table)

    @test result.doc_id == ["doc-a", "doc-b"]
    @test result.passthrough_score == [0.9, 0.5]
    @test metadata["wendao.schema_version"] == WendaoArrow.DEFAULT_SCHEMA_VERSION
end

@testset "Flight stream service delegates processing" begin
    descriptor = WendaoArrow.flight_descriptor(["wendao", "arrow", "stream"])
    service = WendaoArrow.build_stream_flight_service(; descriptor = descriptor) do stream
        doc_ids = String[]
        analyzer_scores = Float64[]
        final_scores = Float64[]

        for batch in stream
            columns = Tables.columntable(batch)
            for (doc_id, vector_score) in zip(columns.doc_id, columns.vector_score)
                score = Float64(vector_score)
                push!(doc_ids, doc_id)
                push!(analyzer_scores, score)
                push!(final_scores, score + 1.0)
            end
        end

        return (
            doc_id = doc_ids,
            analyzer_score = analyzer_scores,
            final_score = final_scores,
        )
    end

    result_table = Arrow.Flight.table(
        service,
        Arrow.Flight.ServerCallContext(),
        sample_table();
        descriptor = descriptor,
        metadata = VALID_SCHEMA_VERSION_METADATA,
    )
    result = Tables.columntable(result_table)
    metadata = WendaoArrow.schema_metadata(result_table)

    @test result.doc_id == ["doc-a", "doc-b"]
    @test result.analyzer_score == [0.9, 0.5]
    @test result.final_score == [1.9, 1.5]
    @test metadata["wendao.schema_version"] == WendaoArrow.DEFAULT_SCHEMA_VERSION
end

@testset "Flight service preserves analyzer schema metadata" begin
    descriptor = WendaoArrow.flight_descriptor(["wendao", "arrow", "metadata"])
    service = WendaoArrow.build_flight_service(; descriptor = descriptor) do table
        columns = Tables.columntable(table)
        source = arrow_table_with_metadata(
            (
                doc_id = collect(columns.doc_id),
                analyzer_score = Float64.(columns.vector_score),
                final_score = Float64.(columns.vector_score),
            );
            metadata = [
                "wendao.schema_version" => "shadowed",
                "analyzer.name" => "flight-schema-metadata-demo",
                "response.mode" => "passthrough",
            ],
            colmetadata = Dict(
                :analyzer_score => ["semantic.role" => "analyzer-score"],
                :final_score => ["semantic.role" => "final-score"],
            ),
        )
        return WendaoArrow.normalize_scoring_response(
            source;
            subject = "flight schema metadata response",
        )
    end

    result_table = Arrow.Flight.table(
        service,
        Arrow.Flight.ServerCallContext(),
        sample_table();
        descriptor = descriptor,
        metadata = VALID_SCHEMA_VERSION_METADATA,
    )
    result = Tables.columntable(result_table)
    metadata = WendaoArrow.schema_metadata(result_table)

    @test result.doc_id == ["doc-a", "doc-b"]
    @test result.analyzer_score == [0.9, 0.5]
    @test result.final_score == [0.9, 0.5]
    @test metadata["wendao.schema_version"] == WendaoArrow.DEFAULT_SCHEMA_VERSION
    @test metadata["analyzer.name"] == "flight-schema-metadata-demo"
    @test metadata["response.mode"] == "passthrough"
    @test column_metadata(result_table, :analyzer_score)["semantic.role"] ==
          "analyzer-score"
    @test column_metadata(result_table, :final_score)["semantic.role"] == "final-score"
end

@testset "Flight service can emit response app metadata" begin
    descriptor = WendaoArrow.flight_descriptor(["wendao", "arrow", "app-metadata"])
    service = WendaoArrow.build_flight_service(; descriptor = descriptor) do table
        columns = Tables.columntable(table)
        partitions = Tables.partitioner(
            Tuple(
                (
                    doc_id = [String(doc_id)],
                    analyzer_score = [Float64(vector_score)],
                    final_score = [Float64(vector_score)],
                ) for
                (doc_id, vector_score) in zip(columns.doc_id, columns.vector_score)
            ),
        )
        return Arrow.Flight.withappmetadata(
            partitions;
            app_metadata = [
                "score-batch:$(index - 1)" for index in eachindex(columns.doc_id)
            ],
        )
    end

    result = Arrow.Flight.table(
        service,
        Arrow.Flight.ServerCallContext(),
        sample_table();
        descriptor = descriptor,
        metadata = VALID_SCHEMA_VERSION_METADATA,
        include_app_metadata = true,
    )
    response_table = result.table
    response_columns = Tables.columntable(response_table)

    @test collect(response_columns.doc_id) == ["doc-a", "doc-b"]
    @test collect(response_columns.analyzer_score) == [0.9, 0.5]
    @test collect(response_columns.final_score) == [0.9, 0.5]
    @test String.(result.app_metadata) == ["score-batch:0", "score-batch:1"]
end

@testset "Flight table service can surface request app metadata" begin
    descriptor =
        WendaoArrow.flight_descriptor(["wendao", "arrow", "request-app-metadata-table"])
    service = WendaoArrow.build_flight_service(;
        descriptor = descriptor,
        include_request_app_metadata = true,
    ) do request
        request_table = request.table
        request_tags = String.(request.app_metadata)
        columns = Tables.columntable(request_table)
        return (
            doc_id = collect(columns.doc_id),
            passthrough_score = Float64.(columns.vector_score),
            request_app_metadata = fill(only(request_tags), length(columns.doc_id)),
        )
    end

    request_source =
        Arrow.Flight.withappmetadata(sample_table(); app_metadata = "request-table:0")
    result_table = Arrow.Flight.table(
        service,
        Arrow.Flight.ServerCallContext(),
        request_source;
        descriptor = descriptor,
        metadata = VALID_SCHEMA_VERSION_METADATA,
    )
    result = Tables.columntable(result_table)

    @test result.doc_id == ["doc-a", "doc-b"]
    @test result.passthrough_score == [0.9, 0.5]
    @test result.request_app_metadata == ["request-table:0", "request-table:0"]
end

@testset "Flight stream service can surface request app metadata" begin
    descriptor =
        WendaoArrow.flight_descriptor(["wendao", "arrow", "request-app-metadata-stream"])
    service = WendaoArrow.build_stream_flight_service(;
        descriptor = descriptor,
        include_request_app_metadata = true,
    ) do stream
        doc_ids = String[]
        analyzer_scores = Float64[]
        request_tags = String[]

        for request_batch in stream
            columns = Tables.columntable(request_batch.table)
            request_tag = String(request_batch.app_metadata)
            for (doc_id, vector_score) in zip(columns.doc_id, columns.vector_score)
                push!(doc_ids, doc_id)
                push!(analyzer_scores, Float64(vector_score))
                push!(request_tags, request_tag)
            end
        end

        return (
            doc_id = doc_ids,
            analyzer_score = analyzer_scores,
            request_app_metadata = request_tags,
        )
    end

    request_source = Arrow.Flight.withappmetadata(
        Tables.partitioner(sample_batches());
        app_metadata = ["request-stream:0", "request-stream:1"],
    )
    result_table = Arrow.Flight.table(
        service,
        Arrow.Flight.ServerCallContext(),
        request_source;
        descriptor = descriptor,
        metadata = VALID_SCHEMA_VERSION_METADATA,
    )
    result = Tables.columntable(result_table)

    @test result.doc_id == ["doc-a", "doc-b", "doc-c"]
    @test result.analyzer_score == [0.9, 0.5, 0.25]
    @test result.request_app_metadata ==
          ["request-stream:0", "request-stream:0", "request-stream:1"]
end

@testset "Flight stream service rejects empty exchange payload" begin
    service = WendaoArrow.build_stream_flight_service(identity)
    request = Channel{Arrow.Flight.Protocol.FlightData}(1)
    close(request)
    response = Channel{Arrow.Flight.Protocol.FlightData}(1)

    @test_throws ArgumentError Arrow.Flight.doexchange(
        service,
        Arrow.Flight.ServerCallContext(),
        request,
        response,
    )
end

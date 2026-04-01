@testset "Flight gRPC server extension loads" begin
    @test Base.get_extension(WendaoArrow, :WendaoArrowgRPCServerExt) !== nothing
end

@testset "flight_server registers Arrow Flight service" begin
    service = WendaoArrow.build_flight_service(identity)
    server = WendaoArrow.flight_server(
        service;
        host = WendaoArrow.DEFAULT_HOST,
        port = available_port(),
        enable_health_check = false,
        enable_reflection = false,
    )

    @test "arrow.flight.protocol.FlightService" in gRPCServer.services(server)
end

@testset "serve_flight starts a non-blocking gRPC listener" begin
    server = WendaoArrow.serve_flight(
        identity;
        host = WendaoArrow.DEFAULT_HOST,
        port = available_port(),
        include_request_app_metadata = true,
        block = false,
    )

    try
        services = Set(gRPCServer.services(server))
        @test "arrow.flight.protocol.FlightService" in services
        @test "grpc.health.v1.Health" in services
        @test "grpc.reflection.v1alpha.ServerReflection" in services
    finally
        gRPCServer.stop!(server; force = true)
    end
end

@testset "serve_stream_flight starts a non-blocking gRPC listener" begin
    server = WendaoArrow.serve_stream_flight(
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
        port = available_port(),
        include_request_app_metadata = true,
        block = false,
    )

    try
        services = Set(gRPCServer.services(server))
        @test "arrow.flight.protocol.FlightService" in services
        @test "grpc.health.v1.Health" in services
    finally
        gRPCServer.stop!(server; force = true)
    end
end

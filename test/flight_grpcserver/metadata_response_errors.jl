@testset "cross-process metadata bad-response Flight startup script surfaces pyarrow DoExchange typed metadata response errors" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_bad_response_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_metadata_doexchange_command(
                python,
                port;
                trace_id = "trace-123",
                tenant_id = "tenant-7",
                attempt_count = 3,
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight response column attempt_count row 1 must contain Int64 values or missing",
            output,
        )
        @test occursin("oops", output)
        @test occursin("::String", output)
        @test !occursin("ArgumentError", output)
    end
end

@testset "cross-process metadata bad-response Flight startup script surfaces native Julia DoExchange typed metadata response errors" begin
    with_metadata_bad_response_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(
            port;
            source = sample_table(),
            metadata = metadata_request_metadata(
                "trace-123";
                tenant_id = "tenant-7",
                attempt_count = 3,
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight response column attempt_count row 1 must contain Int64 values or missing",
            err.message,
        )
        @test occursin("oops", err.message)
        @test occursin("::String", err.message)
        @test !occursin("ArgumentError", err.message)
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test result.grpc_message ==
              "WendaoArrow stream metadata Flight response column attempt_count row 1 must contain Int64 values or missing; got \"oops\"::String"
    end
end

@testset "cross-process metadata bad-enum-response Flight startup script surfaces pyarrow DoExchange enum metadata response errors" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_bad_enum_response_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_metadata_doexchange_command(
                python,
                port;
                trace_id = "trace-123",
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_backend = "remote",
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight response column cache_backend row 1 must contain one of [memory, disk, remote] or missing",
            output,
        )
        @test occursin("sideways", output)
        @test occursin("::String", output)
        @test !occursin("ArgumentError", output)
    end
end

@testset "cross-process metadata bad-enum-response Flight startup script surfaces native Julia DoExchange enum metadata response errors" begin
    with_metadata_bad_enum_response_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(
            port;
            source = sample_table(),
            metadata = metadata_request_metadata(
                "trace-123";
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_backend = "remote",
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight response column cache_backend row 1 must contain one of [memory, disk, remote] or missing",
            err.message,
        )
        @test occursin("sideways", err.message)
        @test occursin("::String", err.message)
        @test !occursin("ArgumentError", err.message)
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test result.grpc_message ==
              "WendaoArrow stream metadata Flight response column cache_backend row 1 must contain one of [memory, disk, remote] or missing; got \"sideways\"::String"
    end
end

@testset "cross-process metadata bad-scope-response Flight startup script surfaces pyarrow DoExchange scope metadata response errors" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_bad_scope_response_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_metadata_doexchange_command(
                python,
                port;
                trace_id = "trace-123",
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_backend = "remote",
                cache_scope = "tenant",
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight response column cache_scope row 1 must contain one of [request, tenant, global] or missing",
            output,
        )
        @test occursin("cluster", output)
        @test occursin("::String", output)
        @test !occursin("ArgumentError", output)
    end
end

@testset "cross-process metadata bad-scope-response Flight startup script surfaces native Julia DoExchange scope metadata response errors" begin
    with_metadata_bad_scope_response_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(
            port;
            source = sample_table(),
            metadata = metadata_request_metadata(
                "trace-123";
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_backend = "remote",
                cache_scope = "tenant",
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight response column cache_scope row 1 must contain one of [request, tenant, global] or missing",
            err.message,
        )
        @test occursin("cluster", err.message)
        @test occursin("::String", err.message)
        @test !occursin("ArgumentError", err.message)
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test result.grpc_message ==
              "WendaoArrow stream metadata Flight response column cache_scope row 1 must contain one of [request, tenant, global] or missing; got \"cluster\"::String"
    end
end

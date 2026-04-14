@testset "cross-process metadata Flight startup script surfaces pyarrow DoExchange invalid ranking-strategy request metadata" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_flight_server() do port, process
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
                ranking_strategy = "graph-only",
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight request metadata key ranking_strategy must contain one of [lexical, semantic, hybrid] or be missing",
            output,
        )
        @test occursin("graph-only", output)
        @test occursin("::String", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process metadata Flight startup script surfaces native Julia DoExchange invalid ranking-strategy request metadata" begin
    with_metadata_flight_server() do port, process
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
                ranking_strategy = "graph-only",
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight request metadata key ranking_strategy must contain one of [lexical, semantic, hybrid] or be missing",
            err.message,
        )
        @test occursin("graph-only", err.message)
        @test occursin("::String", err.message)
        @test matches_expected_invalid_argument_message(
            err.message,
            "WendaoArrow stream metadata Flight request metadata key ranking_strategy must contain one of [lexical, semantic, hybrid] or be missing; got \"graph-only\"::String",
        )
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            "WendaoArrow stream metadata Flight request metadata key ranking_strategy must contain one of [lexical, semantic, hybrid] or be missing; got \"graph-only\"::String",
        )
    end
end

@testset "cross-process metadata Flight startup script surfaces pyarrow DoExchange invalid retrieval-mode request metadata" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_flight_server() do port, process
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
                ranking_strategy = "hybrid",
                retrieval_mode = "semantic_only",
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight request metadata key retrieval_mode must contain one of [graph_only, hybrid, vector_only] or be missing",
            output,
        )
        @test occursin("semantic_only", output)
        @test occursin("::String", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process metadata Flight startup script surfaces native Julia DoExchange invalid retrieval-mode request metadata" begin
    with_metadata_flight_server() do port, process
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
                ranking_strategy = "hybrid",
                retrieval_mode = "semantic_only",
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight request metadata key retrieval_mode must contain one of [graph_only, hybrid, vector_only] or be missing",
            err.message,
        )
        @test occursin("semantic_only", err.message)
        @test occursin("::String", err.message)
        @test matches_expected_invalid_argument_message(
            err.message,
            "WendaoArrow stream metadata Flight request metadata key retrieval_mode must contain one of [graph_only, hybrid, vector_only] or be missing; got \"semantic_only\"::String",
        )
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            "WendaoArrow stream metadata Flight request metadata key retrieval_mode must contain one of [graph_only, hybrid, vector_only] or be missing; got \"semantic_only\"::String",
        )
    end
end

@testset "cross-process metadata Flight startup script surfaces pyarrow DoExchange invalid request metadata" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_metadata_doexchange_command(
                python,
                port;
                trace_id = "trace-123",
                tenant_id = "tenant-7",
                attempt_count = "oops",
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight request metadata key attempt_count must contain Int64 values or be missing",
            output,
        )
        @test occursin("oops", output)
        @test occursin("::String", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process metadata bad-strategy-response Flight startup script surfaces pyarrow DoExchange ranking-strategy metadata response errors" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_bad_strategy_response_flight_server() do port, process
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
                ranking_strategy = "hybrid",
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight response column ranking_strategy row 1 must contain one of [lexical, semantic, hybrid] or missing",
            output,
        )
        @test occursin("vector-only", output)
        @test occursin("::String", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process metadata bad-strategy-response Flight startup script surfaces native Julia DoExchange ranking-strategy metadata response errors" begin
    with_metadata_bad_strategy_response_flight_server() do port, process
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
                ranking_strategy = "hybrid",
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight response column ranking_strategy row 1 must contain one of [lexical, semantic, hybrid] or missing",
            err.message,
        )
        @test occursin("vector-only", err.message)
        @test occursin("::String", err.message)
        @test matches_expected_invalid_argument_message(
            err.message,
            "WendaoArrow stream metadata Flight response column ranking_strategy row 1 must contain one of [lexical, semantic, hybrid] or missing; got \"vector-only\"::String",
        )
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            "WendaoArrow stream metadata Flight response column ranking_strategy row 1 must contain one of [lexical, semantic, hybrid] or missing; got \"vector-only\"::String",
        )
    end
end

@testset "cross-process metadata bad-retrieval-mode-response Flight startup script surfaces pyarrow DoExchange retrieval-mode metadata response errors" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_bad_retrieval_mode_response_flight_server() do port, process
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
                ranking_strategy = "hybrid",
                retrieval_mode = "hybrid",
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight response column retrieval_mode row 1 must contain one of [graph_only, hybrid, vector_only] or missing",
            output,
        )
        @test occursin("semantic_only", output)
        @test occursin("::String", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process metadata bad-retrieval-mode-response Flight startup script surfaces native Julia DoExchange retrieval-mode metadata response errors" begin
    with_metadata_bad_retrieval_mode_response_flight_server() do port, process
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
                ranking_strategy = "hybrid",
                retrieval_mode = "hybrid",
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight response column retrieval_mode row 1 must contain one of [graph_only, hybrid, vector_only] or missing",
            err.message,
        )
        @test occursin("semantic_only", err.message)
        @test occursin("::String", err.message)
        @test matches_expected_invalid_argument_message(
            err.message,
            "WendaoArrow stream metadata Flight response column retrieval_mode row 1 must contain one of [graph_only, hybrid, vector_only] or missing; got \"semantic_only\"::String",
        )
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            "WendaoArrow stream metadata Flight response column retrieval_mode row 1 must contain one of [graph_only, hybrid, vector_only] or missing; got \"semantic_only\"::String",
        )
    end
end

@testset "cross-process metadata Flight startup script surfaces native Julia DoExchange invalid request metadata" begin
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(
            port;
            source = sample_table(),
            metadata = metadata_request_metadata(
                "trace-123";
                tenant_id = "tenant-7",
                attempt_count = "oops",
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight request metadata key attempt_count must contain Int64 values or be missing",
            err.message,
        )
        @test occursin("oops", err.message)
        @test occursin("::String", err.message)
        @test matches_expected_invalid_argument_message(
            err.message,
            "WendaoArrow stream metadata Flight request metadata key attempt_count must contain Int64 values or be missing; got \"oops\"::String",
        )
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            "WendaoArrow stream metadata Flight request metadata key attempt_count must contain Int64 values or be missing; got \"oops\"::String",
        )
    end
end

@testset "cross-process metadata Flight startup script surfaces pyarrow DoExchange duplicate doc_id request errors" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_metadata_doexchange_command(
                python,
                port;
                trace_id = "trace-123",
                tenant_id = "tenant-7",
                attempt_count = 3,
                doc_ids = ("doc-a", "doc-a"),
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight request batch column doc_id row 2 must contain unique non-empty string values",
            output,
        )
        @test occursin("duplicate", output)
        @test occursin("already seen at row 1", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process metadata Flight startup script surfaces native Julia DoExchange duplicate doc_id request errors" begin
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(
            port;
            source = duplicate_doc_id_sample_table(),
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
            "Flight request batch column doc_id row 2 must contain unique non-empty string values",
            err.message,
        )
        @test occursin("duplicate", err.message)
        @test occursin("already seen at row 1", err.message)
        @test matches_expected_invalid_argument_message(
            err.message,
            "WendaoArrow stream metadata Flight request batch column doc_id row 2 must contain unique non-empty string values; duplicate \"doc-a\" already seen at row 1",
        )
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            "WendaoArrow stream metadata Flight request batch column doc_id row 2 must contain unique non-empty string values; duplicate \"doc-a\" already seen at row 1",
        )
    end
end

@testset "cross-process metadata Flight startup script surfaces pyarrow DoExchange invalid enum request metadata" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_metadata_doexchange_command(
                python,
                port;
                trace_id = "trace-123",
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_backend = "sideways",
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight request metadata key cache_backend must contain one of [memory, disk, remote] or be missing",
            output,
        )
        @test occursin("sideways", output)
        @test occursin("::String", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process metadata Flight startup script surfaces native Julia DoExchange invalid enum request metadata" begin
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(
            port;
            source = sample_table(),
            metadata = metadata_request_metadata(
                "trace-123";
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_backend = "sideways",
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight request metadata key cache_backend must contain one of [memory, disk, remote] or be missing",
            err.message,
        )
        @test occursin("sideways", err.message)
        @test occursin("::String", err.message)
        @test matches_expected_invalid_argument_message(
            err.message,
            "WendaoArrow stream metadata Flight request metadata key cache_backend must contain one of [memory, disk, remote] or be missing; got \"sideways\"::String",
        )
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            "WendaoArrow stream metadata Flight request metadata key cache_backend must contain one of [memory, disk, remote] or be missing; got \"sideways\"::String",
        )
    end
end

@testset "cross-process metadata Flight startup script surfaces pyarrow DoExchange invalid scope request metadata" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_metadata_doexchange_command(
                python,
                port;
                trace_id = "trace-123",
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_backend = "remote",
                cache_scope = "cluster",
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight request metadata key cache_scope must contain one of [request, tenant, global] or be missing",
            output,
        )
        @test occursin("cluster", output)
        @test occursin("::String", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process metadata Flight startup script surfaces native Julia DoExchange invalid scope request metadata" begin
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(
            port;
            source = sample_table(),
            metadata = metadata_request_metadata(
                "trace-123";
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_backend = "remote",
                cache_scope = "cluster",
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight request metadata key cache_scope must contain one of [request, tenant, global] or be missing",
            err.message,
        )
        @test occursin("cluster", err.message)
        @test occursin("::String", err.message)
        @test matches_expected_invalid_argument_message(
            err.message,
            "WendaoArrow stream metadata Flight request metadata key cache_scope must contain one of [request, tenant, global] or be missing; got \"cluster\"::String",
        )
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            "WendaoArrow stream metadata Flight request metadata key cache_scope must contain one of [request, tenant, global] or be missing; got \"cluster\"::String",
        )
    end
end

@testset "cross-process metadata Flight startup script surfaces pyarrow DoExchange invalid float request metadata" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_metadata_doexchange_command(
                python,
                port;
                trace_id = "trace-123",
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_hit = true,
                cache_score = "oops",
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight request metadata key cache_score must contain finite Float64 values or be missing",
            output,
        )
        @test occursin("oops", output)
        @test occursin("::String", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process metadata Flight startup script surfaces native Julia DoExchange invalid float request metadata" begin
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(
            port;
            source = sample_table(),
            metadata = metadata_request_metadata(
                "trace-123";
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_hit = true,
                cache_score = "oops",
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight request metadata key cache_score must contain finite Float64 values or be missing",
            err.message,
        )
        @test occursin("oops", err.message)
        @test occursin("::String", err.message)
        @test matches_expected_invalid_argument_message(
            err.message,
            "WendaoArrow stream metadata Flight request metadata key cache_score must contain finite Float64 values or be missing; got \"oops\"::String",
        )
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            "WendaoArrow stream metadata Flight request metadata key cache_score must contain finite Float64 values or be missing; got \"oops\"::String",
        )
    end
end

@testset "cross-process metadata Flight startup script surfaces pyarrow DoExchange invalid datetime request metadata" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_metadata_doexchange_command(
                python,
                port;
                trace_id = "trace-123",
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_hit = true,
                cache_score = 0.75,
                cache_generated_at = "not-a-datetime",
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight request metadata key cache_generated_at must contain ISO8601 DateTime values or be missing",
            output,
        )
        @test occursin("not-a-datetime", output)
        @test occursin("::String", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process metadata Flight startup script surfaces native Julia DoExchange invalid datetime request metadata" begin
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(
            port;
            source = sample_table(),
            metadata = metadata_request_metadata(
                "trace-123";
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_hit = true,
                cache_score = 0.75,
                cache_generated_at = "not-a-datetime",
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight request metadata key cache_generated_at must contain ISO8601 DateTime values or be missing",
            err.message,
        )
        @test occursin("not-a-datetime", err.message)
        @test occursin("::String", err.message)
        @test matches_expected_invalid_argument_message(
            err.message,
            "WendaoArrow stream metadata Flight request metadata key cache_generated_at must contain ISO8601 DateTime values or be missing; got \"not-a-datetime\"::String",
        )
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            "WendaoArrow stream metadata Flight request metadata key cache_generated_at must contain ISO8601 DateTime values or be missing; got \"not-a-datetime\"::String",
        )
    end
end

@testset "cross-process metadata Flight startup script surfaces pyarrow DoExchange invalid bool request metadata" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_metadata_doexchange_command(
                python,
                port;
                trace_id = "trace-123",
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_hit = "maybe",
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight request metadata key cache_hit must contain Bool values or be missing",
            output,
        )
        @test occursin("maybe", output)
        @test occursin("::String", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process metadata Flight startup script surfaces native Julia DoExchange invalid bool request metadata" begin
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(
            port;
            source = sample_table(),
            metadata = metadata_request_metadata(
                "trace-123";
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_hit = "maybe",
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight request metadata key cache_hit must contain Bool values or be missing",
            err.message,
        )
        @test occursin("maybe", err.message)
        @test occursin("::String", err.message)
        @test matches_expected_invalid_argument_message(
            err.message,
            "WendaoArrow stream metadata Flight request metadata key cache_hit must contain Bool values or be missing; got \"maybe\"::String",
        )
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            "WendaoArrow stream metadata Flight request metadata key cache_hit must contain Bool values or be missing; got \"maybe\"::String",
        )
    end
end

@testset "cross-process metadata Flight startup script surfaces pyarrow DoExchange empty trace_id request metadata" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_metadata_doexchange_command(
                python,
                port;
                trace_id = "",
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_hit = true,
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight request metadata key trace_id must contain non-empty string values or be missing",
            output,
        )
        @test occursin("got empty string", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process metadata Flight startup script surfaces native Julia DoExchange empty trace_id request metadata" begin
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(
            port;
            source = sample_table(),
            metadata = metadata_request_metadata(
                "";
                tenant_id = "tenant-7",
                attempt_count = 3,
                cache_hit = true,
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight request metadata key trace_id must contain non-empty string values or be missing",
            err.message,
        )
        @test occursin("got empty string", err.message)
        @test matches_expected_invalid_argument_message(
            err.message,
            "WendaoArrow stream metadata Flight request metadata key trace_id must contain non-empty string values or be missing; got empty string",
        )
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            "WendaoArrow stream metadata Flight request metadata key trace_id must contain non-empty string values or be missing; got empty string",
        )
    end
end

@testset "cross-process metadata Flight startup script surfaces pyarrow DoExchange empty tenant_id request metadata" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_metadata_doexchange_command(
                python,
                port;
                trace_id = "trace-123",
                tenant_id = "",
                attempt_count = 3,
                cache_hit = true,
                expect_error = true,
            ),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight request metadata key tenant_id must contain non-empty string values or be missing",
            output,
        )
        @test occursin("got empty string", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process metadata Flight startup script surfaces native Julia DoExchange empty tenant_id request metadata" begin
    with_metadata_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(
            port;
            source = sample_table(),
            metadata = metadata_request_metadata(
                "trace-123";
                tenant_id = "",
                attempt_count = 3,
                cache_hit = true,
            ),
        )
        err = result.error
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test occursin(
            "Flight request metadata key tenant_id must contain non-empty string values or be missing",
            err.message,
        )
        @test occursin("got empty string", err.message)
        @test matches_expected_invalid_argument_message(
            err.message,
            "WendaoArrow stream metadata Flight request metadata key tenant_id must contain non-empty string values or be missing; got empty string",
        )
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            "WendaoArrow stream metadata Flight request metadata key tenant_id must contain non-empty string values or be missing; got empty string",
        )
    end
end

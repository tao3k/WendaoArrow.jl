@testset "cross-process Flight startup script surfaces pyarrow DoExchange gRPC schema errors" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(pyarrow_bad_schema_doexchange_command(python, port))
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin("requires columns: doc_id, vector_score", output)
        @test occursin("missing: doc_id, vector_score", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process Flight startup script surfaces native Julia DoExchange gRPC schema errors" begin
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(port)
        err = result.error
        expected_message = "WendaoArrow stream scoring Flight request batch requires columns: doc_id, vector_score; missing: doc_id, vector_score"
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(err.message, expected_message)
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            expected_message,
        )
    end
end

@testset "cross-process Flight startup script surfaces pyarrow DoExchange invalid score errors" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(pyarrow_invalid_score_doexchange_command(python, port))
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin("column vector_score row 1 must contain numeric values", output)
        @test occursin("oops", output)
        @test occursin("::String", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process Flight startup script surfaces native Julia DoExchange invalid score errors" begin
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        result =
            native_julia_doexchange_failure(port; source = invalid_score_sample_table())
        err = result.error
        expected_message = "WendaoArrow stream scoring Flight request batch column vector_score row 1 must contain numeric values; got \"oops\"::String"
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(err.message, expected_message)
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            expected_message,
        )
    end
end

@testset "cross-process Flight startup script surfaces pyarrow DoExchange invalid doc_id errors" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        output =
            read_pyarrow_output(pyarrow_invalid_doc_id_doexchange_command(python, port))
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin("column doc_id row 1 must contain string values", output)
        @test occursin("42", output)
        @test occursin("::Int64", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process Flight startup script surfaces native Julia DoExchange invalid doc_id errors" begin
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        result =
            native_julia_doexchange_failure(port; source = invalid_doc_id_sample_table())
        err = result.error
        expected_message = "WendaoArrow stream scoring Flight request batch column doc_id row 1 must contain string values; got 42::Int64"
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(err.message, expected_message)
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            expected_message,
        )
    end
end

@testset "cross-process Flight startup script surfaces pyarrow DoExchange schema-version errors" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(
            pyarrow_invalid_schema_version_doexchange_command(python, port),
        )
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin("requires schema version v1; got v999", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process Flight startup script surfaces native Julia DoExchange schema-version errors" begin
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(
            port;
            source = sample_table(),
            metadata = INVALID_SCHEMA_VERSION_METADATA,
        )
        err = result.error
        expected_message = "Arrow Flight exchange request requires schema version v1; got v999"
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(err.message, expected_message)
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            expected_message,
        )
    end
end

@testset "cross-process Flight startup script surfaces pyarrow DoExchange empty doc_id errors" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        output = read_pyarrow_output(pyarrow_empty_doc_id_doexchange_command(python, port))
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin("column doc_id row 1 must contain non-empty string values", output)
        @test occursin("got empty string", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process Flight startup script surfaces native Julia DoExchange empty doc_id errors" begin
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(port; source = empty_doc_id_sample_table())
        err = result.error
        expected_message = "WendaoArrow stream scoring Flight request batch column doc_id row 1 must contain non-empty string values; got empty string"
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(err.message, expected_message)
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            expected_message,
        )
    end
end

@testset "cross-process Flight startup script surfaces pyarrow DoExchange duplicate doc_id errors" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        output =
            read_pyarrow_output(pyarrow_duplicate_doc_id_doexchange_command(python, port))
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "column doc_id row 2 must contain unique non-empty string values",
            output,
        )
        @test occursin("duplicate", output)
        @test occursin("already seen at row 1", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process Flight startup script surfaces native Julia DoExchange duplicate doc_id errors" begin
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        result =
            native_julia_doexchange_failure(port; source = duplicate_doc_id_sample_table())
        err = result.error
        expected_message = "WendaoArrow stream scoring Flight request batch column doc_id row 2 must contain unique non-empty string values; duplicate \"doc-a\" already seen at row 1"
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(err.message, expected_message)
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            expected_message,
        )
    end
end

@testset "cross-process Flight startup script surfaces pyarrow DoExchange nonfinite score errors" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        output =
            read_pyarrow_output(pyarrow_nonfinite_score_doexchange_command(python, port))
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "column vector_score row 1 must contain finite numeric values",
            output,
        )
        @test occursin("got NaN::Float64", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process Flight startup script surfaces native Julia DoExchange nonfinite score errors" begin
    with_scoring_flight_server() do port, process
        @test Base.process_running(process)
        result =
            native_julia_doexchange_failure(port; source = nonfinite_score_sample_table())
        err = result.error
        expected_message = "WendaoArrow stream scoring Flight request batch column vector_score row 1 must contain finite numeric values; got NaN::Float64"
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(err.message, expected_message)
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            expected_message,
        )
    end
end

@testset "cross-process bad-response Flight startup script surfaces pyarrow DoExchange scoring response errors" begin
    python = locate_pyarrow_flight_python()
    isnothing(python) && error("could not locate pyarrow Flight test environment")
    with_bad_response_flight_server() do port, process
        @test Base.process_running(process)
        output =
            read_pyarrow_output(pyarrow_response_failure_doexchange_command(python, port))
        @test occursin("ok=false", output)
        @test occursin("type=ArrowInvalid", output)
        @test occursin("invalid argument error", lowercase(output))
        @test occursin("grpc_status:3", output)
        @test occursin(
            "Flight response column analyzer_score row 1 must contain finite numeric values",
            output,
        )
        @test occursin("got NaN::Float64", output)
        @test occursin("ArgumentError(", output)
    end
end

@testset "cross-process bad-response Flight startup script surfaces native Julia DoExchange scoring response errors" begin
    with_bad_response_flight_server() do port, process
        @test Base.process_running(process)
        result = native_julia_doexchange_failure(port; source = sample_table())
        err = result.error
        expected_message = "WendaoArrow stream scoring Flight response column analyzer_score row 1 must contain finite numeric values; got NaN::Float64"
        @test err isa gRPCClient.gRPCServiceCallException
        @test err.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(err.message, expected_message)
        @test result.message_count == 0
        @test result.grpc_status == gRPCClient.GRPC_INVALID_ARGUMENT
        @test matches_expected_invalid_argument_message(
            result.grpc_message,
            expected_message,
        )
    end
end

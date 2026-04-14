using Dates

const SPLIT_TEST_ROOT = @__DIR__
const TEST_ROOT = normpath(joinpath(SPLIT_TEST_ROOT, ".."))
const WENDAO_ROOT = normpath(joinpath(TEST_ROOT, ".."))
const FLIGHT_FIXTURE_ROOT = joinpath(TEST_ROOT, "fixtures")
const PARSER_SUMMARY_MODELICA_PACKAGE_FIXTURE =
    joinpath(FLIGHT_FIXTURE_ROOT, "modelica", "Modelica", "Blocks", "package.mo")

function maybe_git_root(path::AbstractString)
    try
        return readchomp(
            pipeline(`git -C $path rev-parse --show-toplevel`; stderr = devnull),
        )
    catch
        return nothing
    end
end

function grpcserver_roots(path::AbstractString)
    roots = String[]
    current = abspath(path)
    while true
        root = maybe_git_root(current)
        if !isnothing(root) && root ∉ roots
            push!(roots, root)
        end
        parent = dirname(current)
        parent == current && break
        current = parent
    end
    return roots
end

function locate_pyarrow_flight_python()
    if haskey(ENV, "ARROW_FLIGHT_PYTHON")
        candidate = abspath(ENV["ARROW_FLIGHT_PYTHON"])
        isfile(candidate) || error("ARROW_FLIGHT_PYTHON does not exist: $candidate")
        pyarrow_flight_available(candidate) ||
            error("ARROW_FLIGHT_PYTHON does not provide pyarrow.flight: $candidate")
        return candidate
    end
    for root in grpcserver_roots(TEST_ROOT)
        for candidate in (
            joinpath(root, ".cache", "arrow-julia-flight-pyenv", "bin", "python"),
            joinpath(root, ".cache", "arrow-julia-flight-pyenv", ".venv", "bin", "python"),
            joinpath(root, ".cache", "wendaosearch-pyarrow-env", "bin", "python"),
            joinpath(root, ".cache", "wendaosearch-pyarrow-env", ".venv", "bin", "python"),
        )
            isfile(candidate) || continue
            pyarrow_flight_available(candidate) && return candidate
        end
    end
    candidate = maybe_uv_pyarrow_launcher()
    isnothing(candidate) && return nothing
    pyarrow_flight_available(candidate) && return candidate
    return nothing
end

function maybe_uv_pyarrow_launcher()
    uv = Sys.which("uv")
    isnothing(uv) && return nothing

    for root in grpcserver_roots(TEST_ROOT)
        launcher = joinpath(root, ".cache", "arrow-julia-flight-pyenv", "bin", "python")
        content = string(
            "#!/bin/sh\n",
            "exec ",
            repr(uv),
            " run --with pyarrow python \"",
            Char('$'),
            "@\"\n",
        )
        mkpath(dirname(launcher))
        if !isfile(launcher) || read(launcher, String) != content
            write(launcher, content)
            chmod(launcher, 0o755)
        end
        return launcher
    end
    return nothing
end

function pyarrow_flight_available(candidate::AbstractString)
    try
        command = `$candidate -c "import pyarrow as pa; import pyarrow.flight as fl"`
        run(pipeline(command; stdout = devnull, stderr = devnull))
        return true
    catch
        return false
    end
end

using Test
using Sockets
using Tables
using WendaoArrow
using gRPCServer
using gRPCClient

const Arrow = WendaoArrow.Arrow

include(joinpath(WENDAO_ROOT, "examples", "support.jl"))

const CACHE_BACKEND_EXTENSION_NAME = "JuliaLang.Enum"
const CACHE_BACKEND_EXTENSION_METADATA = "type=WendaoArrow.CacheBackend;labels=memory:1,disk:2,remote:3"
const CACHE_SCOPE_EXTENSION_NAME = "JuliaLang.Enum"
const CACHE_SCOPE_EXTENSION_METADATA = "type=WendaoArrow.CacheScope;labels=request:1,tenant:2,global:3"
const RANKING_STRATEGY_EXTENSION_NAME = "JuliaLang.Enum"
const RANKING_STRATEGY_EXTENSION_METADATA = "type=WendaoArrow.RankingStrategy;labels=lexical:1,semantic:2,hybrid:3"
const RETRIEVAL_MODE_EXTENSION_NAME = "JuliaLang.Enum"
const RETRIEVAL_MODE_EXTENSION_METADATA = "type=WendaoArrow.LinkGraphRetrievalModes.LinkGraphRetrievalMode;labels=graph_only:1,hybrid:2,vector_only:3"
const NATIVE_JULIA_FLIGHT_DEADLINE = 120
const PYARROW_TRANSIENT_TRANSPORT_ERRORS = (
    "FlightUnavailableError",
    "failed to connect to all addresses",
    "Can't assign requested address",
)

function available_port()
    listener = Sockets.listen(Sockets.localhost, 0)
    address = Sockets.getsockname(listener)
    port = Int(address isa Tuple ? last(address) : address.port)
    close(listener)
    return port
end

function sample_table()
    return (doc_id = ["doc-a", "doc-b"], vector_score = [0.9, 0.5])
end

function parser_summary_fixture_source_text()
    isfile(PARSER_SUMMARY_MODELICA_PACKAGE_FIXTURE) ||
        error("missing parser-summary fixture: $(PARSER_SUMMARY_MODELICA_PACKAGE_FIXTURE)")
    return read(PARSER_SUMMARY_MODELICA_PACKAGE_FIXTURE, String)
end

function parser_summary_request_table(;
    request_id::AbstractString = "req-modelica-package",
    source_id::AbstractString = "Modelica/Blocks/package.mo",
    source_text::AbstractString = parser_summary_fixture_source_text(),
)
    return (
        request_id = [String(request_id)],
        source_id = [String(source_id)],
        source_text = [String(source_text)],
    )
end

function list_roundtrip_sample_table()
    return (
        request_id = ["request-a", "request-b"],
        anchor_values = [String["graph", "retrieval"], String["shared-tag"]],
        edge_kinds = [String["depends_on", "semantic_similar"], String["references"]],
        candidate_node_ids = [String["node-a", "node-b"], String["node-c"]],
    )
end

function expected_list_roundtrip_response()
    return (
        request_id = ["request-a", "request-b"],
        echoed_anchor_values = [String["graph", "retrieval"], String["shared-tag"]],
        echoed_edge_kinds = [
            String["depends_on", "semantic_similar"],
            String["references"],
        ],
        pin_assignment = [String["node-a"], String["node-c"]],
        candidate_size = Int64[2, 1],
    )
end

function invalid_sample_table()
    return (wrong_score = [1.0],)
end

function invalid_score_sample_table()
    return (doc_id = ["doc-a"], vector_score = ["oops"])
end

function invalid_doc_id_sample_table()
    return (doc_id = [42], vector_score = [0.9])
end

function empty_doc_id_sample_table()
    return (doc_id = [""], vector_score = [0.9])
end

function duplicate_doc_id_sample_table()
    return (doc_id = ["doc-a", "doc-a"], vector_score = [0.9, 0.5])
end

function nonfinite_score_sample_table()
    return (doc_id = ["doc-a"], vector_score = [NaN])
end

const VALID_SCHEMA_VERSION_METADATA =
    ["wendao.schema_version" => WendaoArrow.DEFAULT_SCHEMA_VERSION]
const INVALID_SCHEMA_VERSION_METADATA = ["wendao.schema_version" => "v999"]

function sample_batches()
    return (
        (doc_id = ["doc-a", "doc-b"], vector_score = [0.9, 0.5]),
        (doc_id = ["doc-c"], vector_score = [0.25]),
    )
end

function expected_multi_batch_scores()
    return (
        doc_id = ["doc-a", "doc-b", "doc-c"],
        analyzer_score = [0.9, 0.5, 0.25],
        final_score = [0.9, 0.5, 0.25],
    )
end

function wait_for_port(
    host::AbstractString,
    port::Integer,
    process::Base.Process;
    timeout::Float64 = 90.0,
)
    deadline = time() + timeout
    while time() < deadline
        try
            socket = Sockets.connect(host, Int(port))
            close(socket)
            return nothing
        catch
            if !Base.process_running(process)
                wait(process)
                error("Flight server exited before port $port became ready")
            end
            sleep(0.1)
        end
    end
    error("Timed out waiting for Flight server on $host:$port")
end

function is_pyarrow_transient_transport_output(output::AbstractString)
    return any(occursin(pattern, output) for pattern in PYARROW_TRANSIENT_TRANSPORT_ERRORS)
end

function read_pyarrow_output(
    command::Cmd;
    attempts::Integer = 3,
    initial_delay_seconds::Float64 = 0.25,
)
    output = ""
    for attempt = 1:attempts
        output = read(pipeline(ignorestatus(command), stderr = stdout), String)
        if !is_pyarrow_transient_transport_output(output) || attempt == attempts
            return output
        end
        sleep(initial_delay_seconds * attempt)
    end
    return output
end

function terminate_process(process::Base.Process)
    if Base.process_running(process)
        Base.kill(process, Base.SIGTERM)
        deadline = time() + 5.0
        while Base.process_running(process) && time() < deadline
            sleep(0.1)
        end
    end
    if Base.process_running(process)
        Base.kill(process, Base.SIGKILL)
    end
    try
        wait(process)
    catch
    end
    return nothing
end

function _start_flight_server(command_builder::Function; startup_attempts::Integer = 3)
    last_error = nothing
    last_output = ""
    for attempt = 1:startup_attempts
        port = available_port()
        output_path = tempname()
        output_io = open(output_path, "w")
        process = run(
            pipeline(command_builder(port); stdout = output_io, stderr = output_io);
            wait = false,
        )
        close(output_io)
        try
            wait_for_port(WendaoArrow.DEFAULT_HOST, port, process)
            return port, process
        catch err
            last_output = isfile(output_path) ? read(output_path, String) : ""
            if isempty(strip(last_output))
                last_error = err
            else
                message = sprint(showerror, err)
                last_error = ErrorException(
                    string(message, "\nFlight server output:\n", last_output),
                )
            end
            terminate_process(process)
            attempt == startup_attempts || sleep(0.25 * attempt)
        end
    end
    last_error === nothing && error(
        isempty(strip(last_output)) ?
        "WendaoArrow Flight server failed to start without reporting an exception" :
        string(
            "WendaoArrow Flight server failed to start without reporting an exception",
            "\nFlight server output:\n",
            last_output,
        ),
    )
    throw(last_error)
end

function flight_server_command(script_name::AbstractString, port::Integer)
    server_script = joinpath(WENDAO_ROOT, "scripts", script_name)
    repo_root = something(maybe_git_root(TEST_ROOT), dirname(dirname(WENDAO_ROOT)))
    return Cmd(
        Cmd([server_script, "--host", WendaoArrow.DEFAULT_HOST, "--port", string(port)]);
        dir = String(repo_root),
    )
end

function scoring_server_command(port::Integer)
    return flight_server_command("run_stream_scoring_flight_server.sh", port)
end

function large_response_server_command(port::Integer)
    return flight_server_command("run_stream_large_response_flight_server.sh", port)
end

function parser_summary_like_server_command(port::Integer)
    return flight_server_command("run_stream_parser_summary_like_flight_server.sh", port)
end

function parser_summary_request_server_command(port::Integer)
    return flight_server_command("run_stream_parser_summary_request_flight_server.sh", port)
end

function list_roundtrip_server_command(port::Integer)
    return flight_server_command("run_list_roundtrip_flight_server.sh", port)
end

function list_route_roundtrip_server_command(port::Integer)
    return flight_server_command("run_list_route_roundtrip_flight_server.sh", port)
end

function bad_response_server_command(port::Integer)
    return flight_server_command("run_stream_scoring_bad_response_flight_server.sh", port)
end

function metadata_server_command(port::Integer)
    return flight_server_command("run_stream_metadata_flight_server.sh", port)
end

function schema_metadata_server_command(port::Integer)
    return flight_server_command("run_stream_schema_metadata_flight_server.sh", port)
end

function app_metadata_server_command(port::Integer)
    return flight_server_command("run_stream_app_metadata_flight_server.sh", port)
end

function metadata_bad_response_server_command(port::Integer)
    return flight_server_command("run_stream_metadata_bad_response_flight_server.sh", port)
end

function metadata_bad_enum_response_server_command(port::Integer)
    return flight_server_command(
        "run_stream_metadata_bad_enum_response_flight_server.sh",
        port,
    )
end

function metadata_bad_scope_response_server_command(port::Integer)
    return flight_server_command(
        "run_stream_metadata_bad_scope_response_flight_server.sh",
        port,
    )
end

function metadata_bad_strategy_response_server_command(port::Integer)
    return flight_server_command(
        "run_stream_metadata_bad_strategy_response_flight_server.sh",
        port,
    )
end

function metadata_bad_retrieval_mode_response_server_command(port::Integer)
    return flight_server_command(
        "run_stream_metadata_bad_retrieval_mode_response_flight_server.sh",
        port,
    )
end

function with_flight_server(command_builder::Function, f::Function)
    port, process = _start_flight_server(command_builder)
    try
        return f(port, process)
    finally
        terminate_process(process)
    end
end

function with_scoring_flight_server(f::Function)
    return with_flight_server(scoring_server_command, f)
end

function with_large_response_flight_server(f::Function)
    return with_flight_server(large_response_server_command, f)
end

function with_parser_summary_like_flight_server(f::Function)
    return with_flight_server(parser_summary_like_server_command, f)
end

function with_parser_summary_request_flight_server(f::Function)
    return with_flight_server(parser_summary_request_server_command, f)
end

function with_list_roundtrip_flight_server(f::Function)
    return with_flight_server(list_roundtrip_server_command, f)
end

function with_list_route_roundtrip_flight_server(f::Function)
    return with_flight_server(list_route_roundtrip_server_command, f)
end

function with_bad_response_flight_server(f::Function)
    return with_flight_server(bad_response_server_command, f)
end

function with_metadata_flight_server(f::Function)
    return with_flight_server(metadata_server_command, f)
end

function with_schema_metadata_flight_server(f::Function)
    return with_flight_server(schema_metadata_server_command, f)
end

function with_app_metadata_flight_server(f::Function)
    return with_flight_server(app_metadata_server_command, f)
end

function with_metadata_bad_response_flight_server(f::Function)
    return with_flight_server(metadata_bad_response_server_command, f)
end

function with_metadata_bad_enum_response_flight_server(f::Function)
    return with_flight_server(metadata_bad_enum_response_server_command, f)
end

function with_metadata_bad_scope_response_flight_server(f::Function)
    return with_flight_server(metadata_bad_scope_response_server_command, f)
end

function with_metadata_bad_strategy_response_flight_server(f::Function)
    return with_flight_server(metadata_bad_strategy_response_server_command, f)
end

function with_metadata_bad_retrieval_mode_response_flight_server(f::Function)
    return with_flight_server(metadata_bad_retrieval_mode_response_server_command, f)
end

function pyarrow_doexchange_command(
    python::AbstractString,
    port::Integer;
    multi_batch::Bool = false,
)
    code =
        multi_batch ? raw"""
import json
import pyarrow as pa
import pyarrow.flight as fl
import sys

def decode_metadata(field):
    metadata = field.metadata or {}
    return {
        key.decode(): value.decode()
        for key, value in metadata.items()
    }

port = int(sys.argv[1])
client = fl.FlightClient(("127.0.0.1", port))
descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
writer, reader = client.do_exchange(descriptor)
schema = pa.schema(
    [
        pa.field("doc_id", pa.string()),
        pa.field("vector_score", pa.float64()),
    ],
    metadata={b"wendao.schema_version": b"v1"},
)
batch1 = pa.record_batch(
    [
        pa.array(["doc-a", "doc-b"]),
        pa.array([0.9, 0.5]),
    ],
    schema=schema,
)
batch2 = pa.record_batch(
    [
        pa.array(["doc-c"]),
        pa.array([0.25]),
    ],
    schema=schema,
)
writer.begin(batch1.schema)
writer.write_batch(batch1)
writer.write_batch(batch2)
writer.done_writing()
response = reader.read_all()
payload = {
    "doc_id": response.column("doc_id").to_pylist(),
    "analyzer_score": response.column("analyzer_score").to_pylist(),
    "final_score": response.column("final_score").to_pylist(),
}
print(json.dumps(payload, sort_keys=True))
""" :
        raw"""
  import json
  import pyarrow as pa
  import pyarrow.flight as fl
  import sys

  port = int(sys.argv[1])
  client = fl.FlightClient(("127.0.0.1", port))
  descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
  writer, reader = client.do_exchange(descriptor)
  schema = pa.schema(
      [
          pa.field("doc_id", pa.string()),
          pa.field("vector_score", pa.float64()),
      ],
      metadata={b"wendao.schema_version": b"v1"},
  )
  request = pa.Table.from_arrays(
      [
          pa.array(["doc-a", "doc-b"]),
          pa.array([0.9, 0.5]),
      ],
      schema=schema,
  )
  writer.begin(request.schema)
  writer.write_table(request)
  writer.done_writing()
  response = reader.read_all()
  payload = {
      "doc_id": response.column("doc_id").to_pylist(),
      "analyzer_score": response.column("analyzer_score").to_pylist(),
      "final_score": response.column("final_score").to_pylist(),
  }
  print(json.dumps(payload, sort_keys=True))
  """
    return Cmd([python, "-c", code, string(port)])
end

function pyarrow_list_doexchange_command(python::AbstractString, port::Integer)
    code = raw"""
import json
import pyarrow as pa
import pyarrow.flight as fl
import sys

port = int(sys.argv[1])
client = fl.FlightClient(("127.0.0.1", port))
descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
writer, reader = client.do_exchange(descriptor)
schema = pa.schema(
    [
        pa.field("request_id", pa.string()),
        pa.field("anchor_values", pa.list_(pa.string())),
        pa.field("edge_kinds", pa.list_(pa.string())),
        pa.field("candidate_node_ids", pa.list_(pa.string())),
    ],
    metadata={b"wendao.schema_version": b"v1"},
)
request = pa.Table.from_arrays(
    [
        pa.array(["request-a", "request-b"]),
        pa.array([["graph", "retrieval"], ["shared-tag"]], type=pa.list_(pa.string())),
        pa.array([["depends_on", "semantic_similar"], ["references"]], type=pa.list_(pa.string())),
        pa.array([["node-a", "node-b"], ["node-c"]], type=pa.list_(pa.string())),
    ],
    schema=schema,
)
writer.begin(request.schema)
writer.write_table(request)
writer.done_writing()
response = reader.read_all()
payload = {
    "candidate_size": response.column("candidate_size").to_pylist(),
    "echoed_anchor_values": response.column("echoed_anchor_values").to_pylist(),
    "echoed_edge_kinds": response.column("echoed_edge_kinds").to_pylist(),
    "pin_assignment": response.column("pin_assignment").to_pylist(),
    "request_id": response.column("request_id").to_pylist(),
}
print(json.dumps(payload, sort_keys=True))
"""
    return Cmd([python, "-c", code, string(port)])
end

function pyarrow_bad_schema_doexchange_command(python::AbstractString, port::Integer)
    code = raw"""
import pyarrow as pa
import pyarrow.flight as fl
import sys

port = int(sys.argv[1])

try:
    client = fl.FlightClient(("127.0.0.1", port))
    descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
    writer, reader = client.do_exchange(descriptor)
    schema = pa.schema(
        [
            pa.field("wrong_score", pa.float64()),
        ],
        metadata={b"wendao.schema_version": b"v1"},
    )
    request = pa.Table.from_arrays(
        [
            pa.array([1.0]),
        ],
        schema=schema,
    )
    writer.begin(request.schema)
    writer.write_table(request)
    writer.done_writing()
    reader.read_all()
    print("ok=true")
except Exception as exc:
    print("ok=false")
    print(f"type={type(exc).__name__}")
    print(f"message={exc}")
"""
    return Cmd([python, "-c", code, string(port)])
end

function pyarrow_invalid_score_doexchange_command(python::AbstractString, port::Integer)
    code = raw"""
import pyarrow as pa
import pyarrow.flight as fl
import sys

port = int(sys.argv[1])

try:
    client = fl.FlightClient(("127.0.0.1", port))
    descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
    writer, reader = client.do_exchange(descriptor)
    schema = pa.schema(
        [
            pa.field("doc_id", pa.string()),
            pa.field("vector_score", pa.string()),
        ],
        metadata={b"wendao.schema_version": b"v1"},
    )
    request = pa.Table.from_arrays(
        [
            pa.array(["doc-a"]),
            pa.array(["oops"]),
        ],
        schema=schema,
    )
    writer.begin(request.schema)
    writer.write_table(request)
    writer.done_writing()
    reader.read_all()
    print("ok=true")
except Exception as exc:
    print("ok=false")
    print(f"type={type(exc).__name__}")
    print(f"message={exc}")
"""
    return Cmd([python, "-c", code, string(port)])
end

function pyarrow_invalid_doc_id_doexchange_command(python::AbstractString, port::Integer)
    code = raw"""
import pyarrow as pa
import pyarrow.flight as fl
import sys

port = int(sys.argv[1])

try:
    client = fl.FlightClient(("127.0.0.1", port))
    descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
    writer, reader = client.do_exchange(descriptor)
    schema = pa.schema(
        [
            pa.field("doc_id", pa.int64()),
            pa.field("vector_score", pa.float64()),
        ],
        metadata={b"wendao.schema_version": b"v1"},
    )
    request = pa.Table.from_arrays(
        [
            pa.array([42]),
            pa.array([0.9]),
        ],
        schema=schema,
    )
    writer.begin(request.schema)
    writer.write_table(request)
    writer.done_writing()
    reader.read_all()
    print("ok=true")
except Exception as exc:
    print("ok=false")
    print(f"type={type(exc).__name__}")
    print(f"message={exc}")
"""
    return Cmd([python, "-c", code, string(port)])
end

function pyarrow_invalid_schema_version_doexchange_command(
    python::AbstractString,
    port::Integer,
)
    code = raw"""
import pyarrow as pa
import pyarrow.flight as fl
import sys

port = int(sys.argv[1])

try:
    client = fl.FlightClient(("127.0.0.1", port))
    descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
    writer, reader = client.do_exchange(descriptor)
    schema = pa.schema(
        [
            pa.field("doc_id", pa.string()),
            pa.field("vector_score", pa.float64()),
        ],
        metadata={b"wendao.schema_version": b"v999"},
    )
    request = pa.Table.from_arrays(
        [
            pa.array(["doc-a"]),
            pa.array([0.9]),
        ],
        schema=schema,
    )
    writer.begin(request.schema)
    writer.write_table(request)
    writer.done_writing()
    reader.read_all()
    print("ok=true")
except Exception as exc:
    print("ok=false")
    print(f"type={type(exc).__name__}")
    print(f"message={exc}")
"""
    return Cmd([python, "-c", code, string(port)])
end

function pyarrow_empty_doc_id_doexchange_command(python::AbstractString, port::Integer)
    code = raw"""
import pyarrow as pa
import pyarrow.flight as fl
import sys

port = int(sys.argv[1])

try:
    client = fl.FlightClient(("127.0.0.1", port))
    descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
    writer, reader = client.do_exchange(descriptor)
    schema = pa.schema(
        [
            pa.field("doc_id", pa.string()),
            pa.field("vector_score", pa.float64()),
        ],
        metadata={b"wendao.schema_version": b"v1"},
    )
    request = pa.Table.from_arrays(
        [
            pa.array([""]),
            pa.array([0.9]),
        ],
        schema=schema,
    )
    writer.begin(request.schema)
    writer.write_table(request)
    writer.done_writing()
    reader.read_all()
    print("ok=true")
except Exception as exc:
    print("ok=false")
    print(f"type={type(exc).__name__}")
    print(f"message={exc}")
"""
    return Cmd([python, "-c", code, string(port)])
end

function pyarrow_duplicate_doc_id_doexchange_command(python::AbstractString, port::Integer)
    code = raw"""
import pyarrow as pa
import pyarrow.flight as fl
import sys

port = int(sys.argv[1])

try:
    client = fl.FlightClient(("127.0.0.1", port))
    descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
    writer, reader = client.do_exchange(descriptor)
    schema = pa.schema(
        [
            pa.field("doc_id", pa.string()),
            pa.field("vector_score", pa.float64()),
        ],
        metadata={b"wendao.schema_version": b"v1"},
    )
    request = pa.Table.from_arrays(
        [
            pa.array(["doc-a", "doc-a"]),
            pa.array([0.9, 0.5]),
        ],
        schema=schema,
    )
    writer.begin(request.schema)
    writer.write_table(request)
    writer.done_writing()
    reader.read_all()
    print("ok=true")
except Exception as exc:
    print("ok=false")
    print(f"type={type(exc).__name__}")
    print(f"message={exc}")
"""
    return Cmd([python, "-c", code, string(port)])
end

function pyarrow_nonfinite_score_doexchange_command(python::AbstractString, port::Integer)
    code = raw"""
import pyarrow as pa
import pyarrow.flight as fl
import sys

port = int(sys.argv[1])

try:
    client = fl.FlightClient(("127.0.0.1", port))
    descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
    writer, reader = client.do_exchange(descriptor)
    schema = pa.schema(
        [
            pa.field("doc_id", pa.string()),
            pa.field("vector_score", pa.float64()),
        ],
        metadata={b"wendao.schema_version": b"v1"},
    )
    request = pa.Table.from_arrays(
        [
            pa.array(["doc-a"]),
            pa.array([float("nan")]),
        ],
        schema=schema,
    )
    writer.begin(request.schema)
    writer.write_table(request)
    writer.done_writing()
    reader.read_all()
    print("ok=true")
except Exception as exc:
    print("ok=false")
    print(f"type={type(exc).__name__}")
    print(f"message={exc}")
"""
    return Cmd([python, "-c", code, string(port)])
end

function pyarrow_response_failure_doexchange_command(python::AbstractString, port::Integer)
    code = raw"""
import pyarrow as pa
import pyarrow.flight as fl
import sys

port = int(sys.argv[1])

try:
    client = fl.FlightClient(("127.0.0.1", port))
    descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
    writer, reader = client.do_exchange(descriptor)
    schema = pa.schema(
        [
            pa.field("doc_id", pa.string()),
            pa.field("vector_score", pa.float64()),
        ],
        metadata={b"wendao.schema_version": b"v1"},
    )
    request = pa.Table.from_arrays(
        [
            pa.array(["doc-a", "doc-b"]),
            pa.array([0.9, 0.5]),
        ],
        schema=schema,
    )
    writer.begin(request.schema)
    writer.write_table(request)
    writer.done_writing()
    reader.read_all()
    print("ok=true")
except Exception as exc:
    print("ok=false")
    print(f"type={type(exc).__name__}")
    print(f"message={exc}")
"""
    return Cmd([python, "-c", code, string(port)])
end

function pyarrow_metadata_doexchange_command(
    python::AbstractString,
    port::Integer;
    trace_id::Union{Nothing,AbstractString} = nothing,
    tenant_id::Union{Nothing,AbstractString} = nothing,
    attempt_count::Union{Nothing,Integer,AbstractString} = nothing,
    cache_hit::Union{Nothing,Bool,AbstractString} = nothing,
    cache_score::Union{Nothing,Real,AbstractString} = nothing,
    cache_generated_at::Union{Nothing,AbstractString} = nothing,
    cache_backend::Union{Nothing,AbstractString} = nothing,
    cache_scope::Union{Nothing,AbstractString} = nothing,
    ranking_strategy::Union{Nothing,AbstractString} = nothing,
    retrieval_mode::Union{Nothing,AbstractString} = nothing,
    doc_ids = ("doc-a", "doc-b"),
    vector_scores = (0.9, 0.5),
    expect_error::Bool = false,
)
    metadata_parts = String["b\"wendao.schema_version\": b\"v1\""]
    !isnothing(trace_id) &&
        push!(metadata_parts, "b\"trace_id\": $(repr(String(trace_id))).encode()")
    !isnothing(tenant_id) &&
        push!(metadata_parts, "b\"tenant_id\": $(repr(String(tenant_id))).encode()")
    !isnothing(attempt_count) &&
        push!(metadata_parts, "b\"attempt_count\": $(repr(string(attempt_count))).encode()")
    !isnothing(cache_hit) &&
        push!(metadata_parts, "b\"cache_hit\": $(repr(string(cache_hit))).encode()")
    !isnothing(cache_score) &&
        push!(metadata_parts, "b\"cache_score\": $(repr(string(cache_score))).encode()")
    !isnothing(cache_generated_at) && push!(
        metadata_parts,
        "b\"cache_generated_at\": $(repr(String(cache_generated_at))).encode()",
    )
    !isnothing(cache_backend) &&
        push!(metadata_parts, "b\"cache_backend\": $(repr(String(cache_backend))).encode()")
    !isnothing(cache_scope) &&
        push!(metadata_parts, "b\"cache_scope\": $(repr(String(cache_scope))).encode()")
    !isnothing(ranking_strategy) && push!(
        metadata_parts,
        "b\"ranking_strategy\": $(repr(String(ranking_strategy))).encode()",
    )
    !isnothing(retrieval_mode) && push!(
        metadata_parts,
        "b\"retrieval_mode\": $(repr(String(retrieval_mode))).encode()",
    )
    metadata_expr = "{" * join(metadata_parts, ", ") * "}"
    doc_ids_expr = repr(collect(doc_ids))
    vector_scores_expr = repr(collect(vector_scores))

    code =
        expect_error ? """
import pyarrow as pa
import pyarrow.flight as fl
import sys

port = int(sys.argv[1])

try:
    client = fl.FlightClient(("127.0.0.1", port))
    descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
    writer, reader = client.do_exchange(descriptor)
    schema = pa.schema(
        [
            pa.field("doc_id", pa.string()),
            pa.field("vector_score", pa.float64()),
        ],
        metadata=$metadata_expr,
    )
    request = pa.Table.from_arrays(
        [
            pa.array($doc_ids_expr),
            pa.array($vector_scores_expr),
        ],
        schema=schema,
    )
    writer.begin(request.schema)
    writer.write_table(request)
    writer.done_writing()
    reader.read_all()
    print("ok=true")
except Exception as exc:
    print("ok=false")
    print(f"type={type(exc).__name__}")
    print(f"message={exc}")
""" :
        """
  import json
  import pyarrow as pa
  import pyarrow.flight as fl
  import sys

  def decode_metadata(field):
      metadata = field.metadata or {}
      return {
          key.decode(): value.decode()
          for key, value in metadata.items()
      }

  port = int(sys.argv[1])
  client = fl.FlightClient(("127.0.0.1", port))
  descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
  writer, reader = client.do_exchange(descriptor)
  schema = pa.schema(
      [
          pa.field("doc_id", pa.string()),
          pa.field("vector_score", pa.float64()),
      ],
      metadata=$metadata_expr,
  )
  request = pa.Table.from_arrays(
      [
          pa.array($doc_ids_expr),
          pa.array($vector_scores_expr),
      ],
      schema=schema,
  )
  writer.begin(request.schema)
  writer.write_table(request)
  writer.done_writing()
  response = reader.read_all()
  trace_id_field = response.schema.field("trace_id")
  attempt_count_field = response.schema.field("attempt_count")
  cache_backend_field = response.schema.field("cache_backend")
  cache_backend_metadata = cache_backend_field.metadata or {}
  cache_backend_field_metadata = decode_metadata(cache_backend_field)
  cache_scope_field = response.schema.field("cache_scope")
  cache_scope_metadata = cache_scope_field.metadata or {}
  ranking_strategy_field = response.schema.field("ranking_strategy")
  ranking_strategy_metadata = ranking_strategy_field.metadata or {}
  ranking_strategy_field_metadata = decode_metadata(ranking_strategy_field)
  retrieval_mode_field = response.schema.field("retrieval_mode")
  retrieval_mode_metadata = retrieval_mode_field.metadata or {}
  payload = {
      "doc_id": response.column("doc_id").to_pylist(),
      "analyzer_score": response.column("analyzer_score").to_pylist(),
      "final_score": response.column("final_score").to_pylist(),
      "trace_id": response.column("trace_id").to_pylist(),
      "trace_id_field_metadata": decode_metadata(trace_id_field),
      "tenant_id": response.column("tenant_id").to_pylist(),
      "attempt_count": response.column("attempt_count").to_pylist(),
      "attempt_count_field_metadata": decode_metadata(attempt_count_field),
      "cache_backend": response.column("cache_backend").to_pylist(),
      "cache_backend_field_metadata": cache_backend_field_metadata,
      "cache_backend_extension_name": ((cache_backend_metadata.get(b"ARROW:extension:name") or b"").decode() or None),
      "cache_backend_extension_metadata": ((cache_backend_metadata.get(b"ARROW:extension:metadata") or b"").decode() or None),
      "cache_scope": response.column("cache_scope").to_pylist(),
      "cache_scope_extension_name": ((cache_scope_metadata.get(b"ARROW:extension:name") or b"").decode() or None),
      "cache_scope_extension_metadata": ((cache_scope_metadata.get(b"ARROW:extension:metadata") or b"").decode() or None),
      "ranking_strategy": response.column("ranking_strategy").to_pylist(),
      "ranking_strategy_field_metadata": ranking_strategy_field_metadata,
      "ranking_strategy_extension_name": ((ranking_strategy_metadata.get(b"ARROW:extension:name") or b"").decode() or None),
      "ranking_strategy_extension_metadata": ((ranking_strategy_metadata.get(b"ARROW:extension:metadata") or b"").decode() or None),
      "retrieval_mode": response.column("retrieval_mode").to_pylist(),
      "retrieval_mode_extension_name": ((retrieval_mode_metadata.get(b"ARROW:extension:name") or b"").decode() or None),
      "retrieval_mode_extension_metadata": ((retrieval_mode_metadata.get(b"ARROW:extension:metadata") or b"").decode() or None),
      "cache_hit": response.column("cache_hit").to_pylist(),
      "cache_score": response.column("cache_score").to_pylist(),
      "cache_generated_at": [(value.isoformat() if value is not None else None) for value in response.column("cache_generated_at").to_pylist()],
  }
  print(json.dumps(payload, sort_keys=True))
  """
    return Cmd([python, "-c", code, string(port)])
end

function pyarrow_schema_metadata_doexchange_command(
    python::AbstractString,
    port::Integer;
    doc_ids = ("doc-a", "doc-b"),
    vector_scores = (0.9, 0.5),
)
    doc_ids_expr = repr(collect(doc_ids))
    vector_scores_expr = repr(collect(vector_scores))
    code = """
import json
import pyarrow as pa
import pyarrow.flight as fl
import sys

port = int(sys.argv[1])
client = fl.FlightClient(("127.0.0.1", port))
descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
writer, reader = client.do_exchange(descriptor)
schema = pa.schema(
    [
        pa.field("doc_id", pa.string()),
        pa.field("vector_score", pa.float64()),
    ],
    metadata={b"wendao.schema_version": b"v1"},
)
request = pa.Table.from_arrays(
    [
        pa.array($doc_ids_expr),
        pa.array($vector_scores_expr),
    ],
    schema=schema,
)
writer.begin(request.schema)
writer.write_table(request)
writer.done_writing()
response = reader.read_all()
schema_metadata = response.schema.metadata or {}
analyzer_score_field_metadata = response.schema.field("analyzer_score").metadata or {}
final_score_field_metadata = response.schema.field("final_score").metadata or {}
payload = {
    "doc_id": response.column("doc_id").to_pylist(),
    "analyzer_score": response.column("analyzer_score").to_pylist(),
    "final_score": response.column("final_score").to_pylist(),
    "schema_metadata": {key.decode(): value.decode() for key, value in schema_metadata.items()},
    "analyzer_score_field_metadata": {key.decode(): value.decode() for key, value in analyzer_score_field_metadata.items()},
    "final_score_field_metadata": {key.decode(): value.decode() for key, value in final_score_field_metadata.items()},
}
print(json.dumps(payload, sort_keys=True))
"""
    return Cmd([python, "-c", code, string(port)])
end

function pyarrow_app_metadata_doexchange_command(
    python::AbstractString,
    port::Integer;
    doc_ids = ("doc-a", "doc-b"),
    vector_scores = (0.9, 0.5),
)
    doc_ids_expr = repr(collect(doc_ids))
    vector_scores_expr = repr(collect(vector_scores))
    code = """
import json
import pyarrow as pa
import pyarrow.flight as fl
import sys

port = int(sys.argv[1])
client = fl.FlightClient(("127.0.0.1", port))
descriptor = fl.FlightDescriptor.for_path("wendao", "arrow", "exchange")
writer, reader = client.do_exchange(descriptor)
schema = pa.schema(
    [
        pa.field("doc_id", pa.string()),
        pa.field("vector_score", pa.float64()),
    ],
    metadata={b"wendao.schema_version": b"v1"},
)
request = pa.Table.from_arrays(
    [
        pa.array($doc_ids_expr),
        pa.array($vector_scores_expr),
    ],
    schema=schema,
)
writer.begin(request.schema)
writer.write_table(request)
writer.done_writing()

batches = []
app_metadata = []
while True:
    try:
        chunk = reader.read_chunk()
    except StopIteration:
        break
    if chunk.data is None:
        continue
    batches.append(chunk.data)
    app_metadata.append(
        None if chunk.app_metadata is None else chunk.app_metadata.to_pybytes().decode()
    )

response = pa.Table.from_batches(batches)
payload = {
    "doc_id": response.column("doc_id").to_pylist(),
    "analyzer_score": response.column("analyzer_score").to_pylist(),
    "final_score": response.column("final_score").to_pylist(),
    "app_metadata": app_metadata,
}
print(json.dumps(payload, sort_keys=True))
"""
    return Cmd([python, "-c", code, string(port)])
end

function metadata_request_metadata(
    trace_id::Union{Nothing,AbstractString} = nothing;
    tenant_id::Union{Nothing,AbstractString} = nothing,
    attempt_count::Union{Nothing,Integer,AbstractString} = nothing,
    cache_hit::Union{Nothing,Bool,AbstractString} = nothing,
    cache_score::Union{Nothing,Real,AbstractString} = nothing,
    cache_generated_at::Union{Nothing,AbstractString} = nothing,
    cache_backend::Union{Nothing,AbstractString} = nothing,
    cache_scope::Union{Nothing,AbstractString} = nothing,
    ranking_strategy::Union{Nothing,AbstractString} = nothing,
    retrieval_mode::Union{Nothing,AbstractString} = nothing,
)
    metadata =
        Pair{String,String}["wendao.schema_version"=>WendaoArrow.DEFAULT_SCHEMA_VERSION]
    !isnothing(trace_id) && push!(metadata, "trace_id" => String(trace_id))
    !isnothing(tenant_id) && push!(metadata, "tenant_id" => String(tenant_id))
    !isnothing(attempt_count) && push!(metadata, "attempt_count" => string(attempt_count))
    !isnothing(cache_hit) && push!(metadata, "cache_hit" => string(cache_hit))
    !isnothing(cache_score) && push!(metadata, "cache_score" => string(cache_score))
    !isnothing(cache_generated_at) &&
        push!(metadata, "cache_generated_at" => String(cache_generated_at))
    !isnothing(cache_backend) && push!(metadata, "cache_backend" => String(cache_backend))
    !isnothing(cache_scope) && push!(metadata, "cache_scope" => String(cache_scope))
    !isnothing(ranking_strategy) &&
        push!(metadata, "ranking_strategy" => String(ranking_strategy))
    !isnothing(retrieval_mode) &&
        push!(metadata, "retrieval_mode" => String(retrieval_mode))
    return metadata
end

function native_julia_doexchange(
    port::Integer;
    multi_batch::Bool = false,
    metadata = VALID_SCHEMA_VERSION_METADATA,
    client_kwargs...,
)
    return Tables.columntable(
        native_julia_doexchange_table(
            port;
            multi_batch = multi_batch,
            metadata = metadata,
            client_kwargs...,
        ),
    )
end

function native_julia_doexchange_table(
    port::Integer;
    multi_batch::Bool = false,
    source = nothing,
    descriptor = WendaoArrow.flight_descriptor(),
    headers::AbstractVector{<:Pair} = Pair{String,String}[],
    metadata = VALID_SCHEMA_VERSION_METADATA,
    include_app_metadata::Bool = false,
    client_kwargs...,
)
    grpc = gRPCClient.gRPCCURL()
    gRPCClient.grpc_init(grpc)
    try
        client = Arrow.Flight.Client(
            "grpc://127.0.0.1:$(port)";
            grpc = grpc,
            deadline = NATIVE_JULIA_FLIGHT_DEADLINE,
            client_kwargs...,
        )
        normalized_source =
            isnothing(source) ?
            (multi_batch ? Tables.partitioner(sample_batches()) : sample_table()) : source
        req, response = Arrow.Flight.doexchange(
            client,
            normalized_source;
            descriptor = descriptor,
            headers = headers,
            metadata = metadata,
        )
        result = Arrow.Flight.table(
            response;
            convert = true,
            include_app_metadata = include_app_metadata,
        )
        gRPCClient.grpc_async_await(req)
        return result
    finally
        gRPCClient.grpc_shutdown(grpc)
    end
end

function native_julia_channel_doexchange(
    port::Integer;
    multi_batch::Bool = false,
    metadata = VALID_SCHEMA_VERSION_METADATA,
    client_kwargs...,
)
    return Tables.columntable(
        native_julia_channel_doexchange_table(
            port;
            multi_batch = multi_batch,
            metadata = metadata,
            client_kwargs...,
        ),
    )
end

function native_julia_channel_doexchange_table(
    port::Integer;
    multi_batch::Bool = false,
    source = nothing,
    descriptor = WendaoArrow.flight_descriptor(),
    headers::AbstractVector{<:Pair} = Pair{String,String}[],
    metadata = VALID_SCHEMA_VERSION_METADATA,
    include_app_metadata::Bool = false,
    client_kwargs...,
)
    grpc = gRPCClient.gRPCCURL()
    gRPCClient.grpc_init(grpc)
    try
        client = Arrow.Flight.Client(
            "grpc://127.0.0.1:$(port)";
            grpc = grpc,
            deadline = NATIVE_JULIA_FLIGHT_DEADLINE,
            client_kwargs...,
        )
        normalized_source =
            isnothing(source) ?
            (multi_batch ? Tables.partitioner(sample_batches()) : sample_table()) : source
        req, request, response = Arrow.Flight.doexchange(client; headers = headers)
        producer = @async Arrow.Flight.putflightdata!(
            request,
            normalized_source;
            close = true,
            descriptor = descriptor,
            metadata = metadata,
        )
        result = Arrow.Flight.table(
            response;
            convert = true,
            include_app_metadata = include_app_metadata,
        )
        wait(producer)
        gRPCClient.grpc_async_await(req)
        return result
    finally
        gRPCClient.grpc_shutdown(grpc)
    end
end

function product_helper_doexchange(
    port::Integer;
    multi_batch::Bool = false,
    client_kwargs...,
)
    return Tables.columntable(
        product_helper_doexchange_table(port; multi_batch = multi_batch, client_kwargs...),
    )
end

function _schema_wrapped_source(source)
    return WendaoArrow.schema_table(
        source;
        schema_version = WendaoArrow.DEFAULT_SCHEMA_VERSION,
    )
end

function _schema_wrapped_source(source::Tables.Partitioner)
    wrapped = [_schema_wrapped_source(batch) for batch in Tables.partitions(source)]
    return Tables.partitioner(wrapped)
end

function product_helper_doexchange_table(
    port::Integer;
    multi_batch::Bool = false,
    source = nothing,
    descriptor = WendaoArrow.flight_descriptor(),
    headers::AbstractVector{<:Pair} = Pair{String,String}[],
    include_app_metadata::Bool = false,
    client_kwargs...,
)
    client = WendaoArrow.gateway_flight_client(;
        host = WendaoArrow.DEFAULT_HOST,
        port = port,
        deadline = NATIVE_JULIA_FLIGHT_DEADLINE,
        client_kwargs...,
    )
    normalized_source =
        isnothing(source) ?
        (multi_batch ? Tables.partitioner(sample_batches()) : sample_table()) : source
    request = WendaoArrow.flight_exchange_request(
        _schema_wrapped_source(normalized_source);
        descriptor = descriptor,
        headers = headers,
    )
    return WendaoArrow.flight_exchange_table(
        client,
        request;
        convert = true,
        include_app_metadata = include_app_metadata,
    )
end

function native_julia_list_doexchange_table(
    port::Integer;
    metadata = VALID_SCHEMA_VERSION_METADATA,
)
    return native_julia_doexchange_table(
        port;
        source = list_roundtrip_sample_table(),
        metadata = metadata,
    )
end

function _collect_string_lists(values)
    return [String[item for item in value] for value in values]
end

function assert_list_roundtrip_columns(
    response_table,
    expected;
    response_mode::AbstractString = "list-roundtrip",
)
    columns = Tables.columntable(response_table)
    @test collect(columns.request_id) == expected.request_id
    @test _collect_string_lists(columns.echoed_anchor_values) ==
          expected.echoed_anchor_values
    @test _collect_string_lists(columns.echoed_edge_kinds) == expected.echoed_edge_kinds
    @test _collect_string_lists(columns.pin_assignment) == expected.pin_assignment
    @test Int64.(collect(columns.candidate_size)) == expected.candidate_size
    metadata = WendaoArrow.schema_metadata(response_table)
    @test metadata["wendao.schema_version"] == WendaoArrow.DEFAULT_SCHEMA_VERSION
    @test metadata["response.mode"] == response_mode
end

function search_like_structural_route_headers()
    return Pair{String,String}[
        "x-wendao-schema-version"=>"v1",
        "x-wendao-search-route-name"=>"structural_rerank",
        "x-wendao-search-route-path"=>"/graph/structural/rerank",
        "x-wendao-search-route-status"=>"package_local_draft",
        "x-wendao-search-payload-kind"=>"request",
        "x-wendao-search-contract-shape"=>"structural_rerank_request",
        "x-wendao-search-query-id"=>"query-route-probe",
        "x-trace-id"=>"trace-route-probe",
    ]
end

function native_julia_request_headers_supported()
    extension = Base.get_extension(Arrow, :ArrowFlightgRPCClientExt)
    return !isnothing(extension) && isdefined(extension, Symbol("_header_lines"))
end

_grpc_request(req) = hasproperty(req, :request) ? getproperty(req, :request) : req
_grpc_status(req) = getproperty(_grpc_request(req), :grpc_status)

wrapped_argumenterror_text(message::AbstractString) =
    "ArgumentError(" * repr(String(message)) * ")"

matches_expected_invalid_argument_message(
    message::AbstractString,
    expected::AbstractString,
) = message == expected || message == wrapped_argumenterror_text(expected)
_grpc_message(req) = getproperty(_grpc_request(req), :grpc_message)

function native_julia_doexchange_failure(
    port::Integer;
    source = invalid_sample_table(),
    metadata = VALID_SCHEMA_VERSION_METADATA,
    attempts::Integer = 2,
    retry_delay_seconds::Float64 = 0.25,
)
    last_result = nothing
    for attempt = 1:attempts
        result = _native_julia_doexchange_failure_once(
            port;
            source = source,
            metadata = metadata,
        )
        last_result = result
        _is_transient_native_julia_deadline(result) || return result
        attempt == attempts || sleep(retry_delay_seconds * attempt)
    end
    return last_result
end

function _is_transient_native_julia_deadline(result)
    err = getproperty(result, :error)
    return err isa gRPCClient.gRPCServiceCallException &&
           err.grpc_status == gRPCClient.GRPC_DEADLINE_EXCEEDED &&
           err.message == "Deadline exceeded." &&
           getproperty(result, :grpc_status) == 0 &&
           isempty(getproperty(result, :grpc_message))
end

function _native_julia_doexchange_failure_once(
    port::Integer;
    source = invalid_sample_table(),
    metadata = VALID_SCHEMA_VERSION_METADATA,
)
    grpc = gRPCClient.gRPCCURL()
    gRPCClient.grpc_init(grpc)
    try
        client = Arrow.Flight.Client(
            "grpc://127.0.0.1:$(port)";
            grpc = grpc,
            deadline = NATIVE_JULIA_FLIGHT_DEADLINE,
        )
        req, response = Arrow.Flight.doexchange(
            client,
            source;
            descriptor = WendaoArrow.flight_descriptor(),
            metadata = metadata,
        )
        try
            Arrow.Flight.table(response)
            gRPCClient.grpc_async_await(req)
            return (
                error = nothing,
                message_count = -1,
                grpc_status = _grpc_status(req),
                grpc_message = _grpc_message(req),
            )
        catch err
            grpc_error = try
                gRPCClient.grpc_async_await(req)
                nothing
            catch caught
                caught
            end
            return (
                error = isnothing(grpc_error) ? err : grpc_error,
                message_count = 0,
                grpc_status = _grpc_status(req),
                grpc_message = _grpc_message(req),
            )
        end
    finally
        gRPCClient.grpc_shutdown(grpc)
    end
end

function assert_scoring_columns(
    response,
    expected_doc_ids,
    expected_analyzer_scores,
    expected_final_scores,
)
    @test collect(response.doc_id) == expected_doc_ids
    @test collect(response.analyzer_score) == expected_analyzer_scores
    @test collect(response.final_score) == expected_final_scores
end

function assert_metadata_scoring_columns(
    response,
    expected_trace_ids,
    expected_tenant_ids,
    expected_attempt_counts,
    expected_cache_backends,
    expected_cache_scopes,
    expected_ranking_strategies,
    expected_retrieval_modes,
    expected_cache_hits,
    expected_cache_scores,
    expected_cache_generated_ats,
)
    assert_scoring_columns(response, ["doc-a", "doc-b"], [0.9, 0.5], [0.9, 0.5])
    @test isequal(collect(response.trace_id), expected_trace_ids)
    @test isequal(collect(response.tenant_id), expected_tenant_ids)
    @test isequal(collect(response.attempt_count), expected_attempt_counts)
    @test Base.nonmissingtype(eltype(response.cache_backend)) == WendaoArrow.CacheBackend
    @test isequal(collect(response.cache_backend), expected_cache_backends)
    @test Base.nonmissingtype(eltype(response.cache_scope)) == WendaoArrow.CacheScope
    @test isequal(collect(response.cache_scope), expected_cache_scopes)
    @test Base.nonmissingtype(eltype(response.ranking_strategy)) ==
          WendaoArrow.RankingStrategy
    @test isequal(collect(response.ranking_strategy), expected_ranking_strategies)
    @test Base.nonmissingtype(eltype(response.retrieval_mode)) ==
          WendaoArrow.LinkGraphRetrievalMode
    @test isequal(collect(response.retrieval_mode), expected_retrieval_modes)
    @test isequal(collect(response.cache_hit), expected_cache_hits)
    @test isequal(collect(response.cache_score), expected_cache_scores)
    @test isequal(collect(response.cache_generated_at), expected_cache_generated_ats)
end

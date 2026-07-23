"""
Generated gRPC stubs for `gcon_transport.proto`.

`gcon_transport_pb2.py` and `gcon_transport_pb2_grpc.py` are checked
in (generated code, not hand-written) so the package works without
requiring `grpcio-tools` at runtime -- only `grpcio` itself is a
runtime dependency; `grpcio-tools` is only needed if you edit the
`.proto` file and need to regenerate. To regenerate after editing
`gcon_transport.proto`:

    python -m grpc_tools.protoc -I src/gcon/transport/proto \
        --python_out=src/gcon/transport/proto \
        --grpc_python_out=src/gcon/transport/proto \
        src/gcon/transport/proto/gcon_transport.proto

    # then fix the generated import in gcon_transport_pb2_grpc.py:
    #   import gcon_transport_pb2 as gcon__transport__pb2
    # ->  from gcon.transport.proto import gcon_transport_pb2 as gcon__transport__pb2
"""

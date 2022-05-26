# hstreamdb-py

Still in development

```sh
virtualenv -p python3 ./venv
source venv/bin/activate

pip install grpcio-tools
```

```sh
python -m grpc_tools.protoc -I./protos \
  --python_out=src \
  --grpc_python_out=src \
  ./protos/HStreamApi.proto
```

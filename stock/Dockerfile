FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY . .

# Generate the gRPC stubs from services.proto
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. services.proto

EXPOSE 5000

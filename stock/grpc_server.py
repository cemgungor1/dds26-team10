import threading
from concurrent import futures

import grpc
import redis

from msgspec import msgpack

import services_pb2
import services_pb2_grpc

import app as flask_app

DB_ERROR_STR = "DB error"

class StockServicer(services_pb2_grpc.StockServiceServicer):
    # gRPC Protobuff version of the HTTP Request
    def FindItem(self, request, context):
        try:
            entry = flask_app.db.get(request.item_id)
        except redis.exceptions.RedisError:
            context.abort(grpc.StatusCode.INTERNAL, DB_ERROR_STR)

        if entry is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Item: {request.item_id} not found!")

        item = msgpack.decode(entry, type=flask_app.StockValue)
        return services_pb2.FindItemReply(
            item_id = request.item_id,
            price = item.price,
            stock = item.stock
        )

    # gRPC Protobuff version of the HTTP Request
    def SubtractStock(self, request, context):
        try:
            entry = flask_app.db.get(request.item_id)
        except redis.exceptions.RedisError:
            context.abort(grpc.StatusCode.INTERNAL, DB_ERROR_STR)

        if entry is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Item: {request.item_id} not found!")

        item = msgpack.decode(entry, type=flask_app.StockValue)
        item.stock -= request.quantity

        if item.stock < 0:
            return services_pb2.StockReply(
                success=False,
                message=f"Item: {request.item_id} stock cannot get reduced below zero!"
            )

        try:
            flask_app.db.set(request.item_id, msgpack.encode(item))
        except redis.exceptions.RedisError:
            context.abort(grpc.StatusCode.INTERNAL, DB_ERROR_STR)

        return services_pb2.StockReply(success=True, message="OK")

    # gRPC Protobuff version of the HTTP Request
    def AddStock(self, request, context):
        try:
            entry = flask_app.db.get(request.item_id)
        except redis.exceptions.RedisError:
            context.abort(grpc.StatusCode.INTERNAL, DB_ERROR_STR)
        
        if entry is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Item: {request.item_id} not found!")

        item = msgpack.decode(entry, type=flask_app.StockValue)
        item.stock += request.quantity

        try:
            flask_app.db.set(request.item_id, msgpack.encode(item))
        except redis.exceptions.RedisError:
            context.abort(grpc.StatusCode.INTERNAL, DB_ERROR_STR)

        return services_pb2.StockReply(success=True, message="OK")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    services_pb2_grpc.add_StockServiceServicer_to_server(StockServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    flask_app.app.logger.info("gRPC stock server started on port 50051")
    server.wait_for_termination()

def start_grpc_server():
    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

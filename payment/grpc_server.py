import threading
from concurrent import futures

import grpc
import redis

from msgspec import msgpack

import services_pb2
import services_pb2_grpc

import app as flask_app

DB_ERROR_STR = "DB error"

class PaymentServicer(services_pb2_grpc.PaymentServiceServicer):
    # gRPC Protobuff version of the HTTP Request
    def Pay(self, request, context):
        try:
            entry = flask_app.db.get(request.user_id)
        except redis.exceptions.RedisError:
            context.abort(grpc.StatusCode.INTERNAL, DB_ERROR_STR)

        if entry is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"User: {request.user_id} not found!")

        user = msgpack.decode(entry, type=flask_app.UserValue)
        user.credit -= request.amount

        if user.credit < 0:
            return services_pb2.PayReply(
                success=False,
                message=f"User: {request.user_id} credit cannot get reduced below zero!"
            )
        
        # Return success!
        try:
            flask_app.db.set(request.user_id, msgpack.encode(user))
        except redis.exceptions.RedisError:
            context.abort(grpc.StatusCode.INTERNAL, DB_ERROR_STR)

        return services_pb2.PayReply(success=True, message="OK")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    services_pb2_grpc.add_PaymentServiceServicer_to_server(PaymentServicer(), server)
    server.add_insecure_port("[::]:50052")
    server.start()
    flask_app.app.logger.info("gRPC payment server started on port 50052")
    server.wait_for_termination()

def start_grpc_server():
    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

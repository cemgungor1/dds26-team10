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
        for _attempt in range(5):
            try:
                with flask_app.db.pipeline() as pipe:
                    pipe.watch(request.user_id)
                    raw = pipe.get(request.user_id)
                    if raw is None:
                        pipe.unwatch()
                        context.abort(grpc.StatusCode.NOT_FOUND, f"User: {request.user_id} not found!")
                    user = msgpack.decode(raw, type=flask_app.UserValue)
                    user.credit -= request.amount
                    if user.credit < 0:
                        pipe.unwatch()
                        return services_pb2.PayReply(
                            success=False,
                            message=f"User: {request.user_id} credit cannot get reduced below zero!"
                        )
                    pipe.multi()
                    pipe.set(request.user_id, msgpack.encode(user))
                    pipe.execute()
                    return services_pb2.PayReply(success=True, message="OK")
            except redis.exceptions.WatchError:
                continue
            except redis.exceptions.RedisError:
                context.abort(grpc.StatusCode.INTERNAL, DB_ERROR_STR)
        context.abort(grpc.StatusCode.INTERNAL, DB_ERROR_STR)

    # gRPC Protobuff version of the HTTP Request - Refund
    def Refund(self, request, context):
        for _attempt in range(5):
            try:
                with flask_app.db.pipeline() as pipe:
                    pipe.watch(request.user_id)
                    raw = pipe.get(request.user_id)
                    if raw is None:
                        pipe.unwatch()
                        context.abort(grpc.StatusCode.NOT_FOUND, f"User: {request.user_id} not found!")
                    user = msgpack.decode(raw, type=flask_app.UserValue)
                    user.credit += request.amount
                    pipe.multi()
                    pipe.set(request.user_id, msgpack.encode(user))
                    pipe.execute()
                    return services_pb2.PayReply(success=True, message="OK")
            except redis.exceptions.WatchError:
                continue
            except redis.exceptions.RedisError:
                context.abort(grpc.StatusCode.INTERNAL, DB_ERROR_STR)
        context.abort(grpc.StatusCode.INTERNAL, DB_ERROR_STR)

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

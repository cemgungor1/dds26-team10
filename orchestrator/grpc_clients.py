import os
import grpc

import services_pb2_grpc


def _get_env_var(name: str) -> str:
    """
    Retrieve a required environment variable or raise a clear error if missing.
    """
    value = os.environ.get(name)
    if value is None:
        raise RuntimeError(f"Required environment variable '{name}' is not set.")
    return value


# Stock Channel Stub
_stock_channel = grpc.insecure_channel(
    f"{_get_env_var('STOCK_GRPC_HOST')}:{_get_env_var('STOCK_GRPC_PORT')}"
)
stock_stub = services_pb2_grpc.StockServiceStub(_stock_channel)

# Payment Channel Stub
_payment_channel = grpc.insecure_channel(
    f"{_get_env_var('PAYMENT_GRPC_HOST')}:{_get_env_var('PAYMENT_GRPC_PORT')}"
)
payment_stub = services_pb2_grpc.PaymentServiceStub(_payment_channel)

import os
import grpc

import services_pb2_grpc

# Stock Channel Stub
_stock_channel = grpc.insecure_channel(
    f"{os.environ['STOCK_GRPC_HOST']}:{os.environ['STOCK_GRPC_PORT']}"
)
stock_stub = services_pb2_grpc.StockServiceStub(_stock_channel)

# Payment Channel Stub
_payment_channel = grpc.insecure_channel(
    f"{os.environ['PAYMENT_GRPC_HOST']}:{os.environ['PAYMENT_GRPC_PORT']}"
)
payment_stub = services_pb2_grpc.PaymentServiceStub(_payment_channel)

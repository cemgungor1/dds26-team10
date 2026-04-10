def post_fork(server, worker):
    from app import start_background_services
    from grpc_server import start_grpc_server
    start_background_services()
    start_grpc_server()

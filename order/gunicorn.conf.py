def post_fork(server, worker):
    from app import start_background_services
    start_background_services()

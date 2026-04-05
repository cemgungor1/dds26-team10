def on_starting(server):
    from app import start_background_services
    start_background_services()

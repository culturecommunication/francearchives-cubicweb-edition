def teardown_module(module):
    print("tearing down module", module.__name__)
    from elasticsearch_dsl.connections import connections

    # remove cached connection created by elasticsearch_dsl
    # after ``get_connection()`` was called
    try:
        connections.remove_connection("default")
    except KeyError:
        # if for some reason connection wasn't created, don't crash, this is
        # just a cleanup function
        pass

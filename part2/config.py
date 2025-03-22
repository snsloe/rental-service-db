import rethinkdb as r

def get_connection():
    return r.RethinkDB().connect(host='localhost', port=28015, db='rental_service')

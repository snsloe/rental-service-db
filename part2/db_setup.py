import rethinkdb as r
from config import get_connection

rdb = r.RethinkDB()
conn = get_connection()


try:
    rdb.db_drop('rental_service').run(conn)
    print("Старая база `rental_service` удалена.")
except Exception as e:
    print("База отсутствует или уже удалена.")


try:
    rdb.db_create('rental_service').run(conn)
    print("База `rental_service` создана.")
except:
    print("База уже существует.")


tables = ['customers', 'rental_points', 'items', 'rentals']
for table in tables:
    try:
        rdb.db('rental_service').table_create(table).run(conn)
        print(f"Таблица `{table}` создана.")
    except:
        print(f"Таблица `{table}` уже существует.")

conn.close()

import rethinkdb as r
from config import get_connection

rdb = r.RethinkDB()
conn = rdb.connect(host='localhost', port=28015)

customers_data = [
    {"id": "001", "last_name": "Жалнин", "district": "Приокский", "discount": 2},
    {"id": "002", "last_name": "Семенов", "district": "Советский", "discount": 6},
    {"id": "003", "last_name": "Кожаков", "district": "Ленинский", "discount": 0},
    {"id": "004", "last_name": "Шерстнев", "district": "Автозаводский", "discount": 0},
    {"id": "005", "last_name": "Козлов", "district": "Нижегородский", "discount": 4},
]

try:
    rdb.db('rental_service').table('customers').insert(customers_data).run(conn)
    print("Данные успешно добавлены в `customers`")
except Exception as e:
    print(f"Ошибка при вставке в `customers`: {e}")

rental_points_data = [
    {"id": "001", "number": "N23", "location": "Нижегородский", "commission": 4},
    {"id": "002", "number": "N16", "location": "Советский", "commission": 5},
    {"id": "003", "number": "N8", "location": "Сормовский", "commission": 7},
    {"id": "004", "number": "N21", "location": "Приокский", "commission": 3},
    {"id": "005", "number": "N12", "location": "Нижегородский", "commission": 2},
    {"id": "006", "number": "N6", "location": "Канавинский", "commission": 5},
]

try:
    rdb.db('rental_service').table('rental_points').insert(rental_points_data).run(conn)
    print("Данные успешно добавлены в `rental_points`")
except Exception as e:
    print(f"Ошибка при вставке в `rental_points`: {e}")

items_data = [
    {"id": "001", "name": "Телевизор", "storage": "Нижегородский", "quantity": 7, "rental_price": 10000},
    {"id": "002", "name": "Часы напольные", "storage": "Советский", "quantity": 6, "rental_price": 5000},
    {"id": "003", "name": "Радиоприемник", "storage": "Нижегородский", "quantity": 10, "rental_price": 7000},
    {"id": "004", "name": "Часы настенные", "storage": "Приокский", "quantity": 20, "rental_price": 3000},
    {"id": "005", "name": "Холодильник", "storage": "Сормовский", "quantity": 6, "rental_price": 12000},
    {"id": "006", "name": "Утюг", "storage": "Нижегородский", "quantity": 30, "rental_price": 2000},
    {"id": "007", "name": "Весы детские", "storage": "Нижегородский", "quantity": 15, "rental_price": 1500},
]

try:
    rdb.db('rental_service').table('items').insert(items_data).run(conn)
    print("Данные успешно добавлены в `items`")
except Exception as e:
    print(f"Ошибка при вставке в `items`: {e}")

rentals_data = [
    {"id": "10005", "customer_id": "002", "date": "Январь", "rental_point_id": "003", "item_id": "003", "rental_period": 4, "amount": 28000},
    {"id": "10006", "customer_id": "003", "date": "Январь", "rental_point_id": "003", "item_id": "007", "rental_period": 1, "amount": 1500},
    {"id": "10007", "customer_id": "004", "date": "Январь", "rental_point_id": "002", "item_id": "006", "rental_period": 8, "amount": 16000},
    {"id": "10008", "customer_id": "003", "date": "Февраль", "rental_point_id": "002", "item_id": "005", "rental_period": 4, "amount": 48000},
    {"id": "10009", "customer_id": "004", "date": "Февраль", "rental_point_id": "001", "item_id": "001", "rental_period": 4, "amount": 40000},
    {"id": "10010", "customer_id": "005", "date": "Март", "rental_point_id": "003", "item_id": "006", "rental_period": 4, "amount": 8000},
    {"id": "10011", "customer_id": "005", "date": "Март", "rental_point_id": "006", "item_id": "003", "rental_period": 8, "amount": 56000},
    {"id": "10012", "customer_id": "001", "date": "Апрель", "rental_point_id": "003", "item_id": "003", "rental_period": 8, "amount": 56000},
    {"id": "10013", "customer_id": "004", "date": "Апрель", "rental_point_id": "004", "item_id": "002", "rental_period": 2, "amount": 10000},
    {"id": "10014", "customer_id": "002", "date": "Май", "rental_point_id": "005", "item_id": "007", "rental_period": 2, "amount": 3000},
    {"id": "10015", "customer_id": "004", "date": "Май", "rental_point_id": "006", "item_id": "004", "rental_period": 1, "amount": 3000},
    {"id": "10016", "customer_id": "004", "date": "Май", "rental_point_id": "002", "item_id": "001", "rental_period": 11, "amount": 110000},
    {"id": "10017", "customer_id": "003", "date": "Июнь", "rental_point_id": "001", "item_id": "001", "rental_period": 1, "amount": 10000},
    {"id": "10018", "customer_id": "005", "date": "Июль", "rental_point_id": "001", "item_id": "007", "rental_period": 1, "amount": 1500},
    {"id": "10019", "customer_id": "003", "date": "Август", "rental_point_id": "003", "item_id": "002", "rental_period": 4, "amount": 20000},
    {"id": "10020", "customer_id": "004", "date": "Август", "rental_point_id": "005", "item_id": "002", "rental_period": 4, "amount": 20000},
    {"id": "10021", "customer_id": "001", "date": "Август", "rental_point_id": "003", "item_id": "001", "rental_period": 2, "amount": 20000},
]

try:
    rdb.db('rental_service').table('rentals').insert(rentals_data).run(conn)
    print("Данные успешно добавлены в `rentals`")
except Exception as e:
    print(f"Ошибка при вставке в `rentals`: {e}")

conn.close()
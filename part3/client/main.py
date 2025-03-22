import requests

BASE_URL = "http://127.0.0.1:8000"

def print_customers(): # текущее состояние таблицы заказчиков
    response = requests.get(f"{BASE_URL}/customers/")
    print("\nСписок заказчиков:")
    print(response.json())

print("\nЗапуск скрипта: \n")

# 1. Вставка данных
print("Добавляем новых заказчиков...")
requests.post(f"{BASE_URL}/insert/", params={"id": "006", "last_name": "Петров", "district": "Ленинский", "discount": 5})
requests.post(f"{BASE_URL}/insert/", params={"id": "007", "last_name": "Иванов", "district": "Автозаводский", "discount": 3})
print_customers()

# 2. Обновление скидки
print("\nОбновляем скидку Иванова до 10%...")
requests.put(f"{BASE_URL}/update/", params={"last_name": "Иванов", "new_discount": 10})
print_customers()

# 3. Удаление заказчика
print("\nУдаляем Петрова...")
requests.delete(f"{BASE_URL}/delete/", params={"last_name": "Петров"})
print_customers()

# 4. Очистка таблицы
print("\nОчищаем всю таблицу заказчиков...")
requests.delete(f"{BASE_URL}/clear/")
print_customers()

print("\nУспешное завершение")

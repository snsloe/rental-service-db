from fastapi import FastAPI
from rethinkdb import RethinkDB

# Подключение к RethinkDB
rdb = RethinkDB()
DB_NAME = "rental_service"
TABLE_NAME = "customers"

app = FastAPI()

def get_connection():
    try:
        conn = rdb.connect(host="localhost", port=28015, db=DB_NAME)
        return conn
    except Exception as e:
        print(f"Ошибка подключения к RethinkDB: {e}")
        return None


from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Проверяет существование таблицы при запуске"""
    conn = get_connection()
    if conn:
        try:
            if TABLE_NAME not in rdb.db(DB_NAME).table_list().run(conn):
                print(f"Таблица `{TABLE_NAME}` не найдена. Возможно, нужно выполнить `db_setup.py`.")
        except Exception as e:
            print(f"Ошибка при проверке таблицы: {e}")
        finally:
            conn.close()
    yield  # Переход к запуску FastAPI

app = FastAPI(lifespan=lifespan)

# добавление заказчика
@app.post("/insert/")
def insert_customer(id: str, last_name: str, district: str, discount: int):
    conn = get_connection()
    if not conn:
        return {"error": "Не удалось подключиться к базе данных"}
    customer = {"id": id, "last_name": last_name, "district": district, "discount": discount}
    try:
        rdb.db(DB_NAME).table(TABLE_NAME).insert(customer).run(conn)
        return {"message": f"Заказчик {last_name} добавлен"}
    except Exception as e:
        return {"error": f"Ошибка при добавлении заказчика: {e}"}
    finally:
        conn.close()


# Выводит список всех заказчиков
@app.get("/customers/")
def get_customers():
    conn = get_connection()
    if not conn:
        return {"error": "Не удалось подключиться к базе данных"}
    try:
        data = list(rdb.db(DB_NAME).table(TABLE_NAME).run(conn))
        return data
    except Exception as e:
        return {"error": f"Ошибка при получении данных: {e}"}
    finally:
        conn.close()

# Обновляет скидку заказчика по фамилии
@app.put("/update/")
def update_discount(last_name: str, new_discount: int):
    conn = get_connection()
    if not conn:
        return {"error": "Не удалось подключиться к базе данных"}
    try:
        result = rdb.db(DB_NAME).table(TABLE_NAME).filter({"last_name": last_name}).update(
            {"discount": new_discount}).run(conn)
        if result["replaced"] == 0:
            return {"message": f"Заказчик {last_name} не найден"}
        return {"message": f"Скидка {last_name} обновлена до {new_discount}%"}
    except Exception as e:
        return {"error": f"Ошибка при обновлении данных: {e}"}
    finally:
        conn.close()

#Удаляет заказчика по фамилии
@app.delete("/delete/")
def delete_customer(last_name: str):
    conn = get_connection()
    if not conn:
        return {"error": "Не удалось подключиться к базе данных"}
    try:
        result = rdb.db(DB_NAME).table(TABLE_NAME).filter({"last_name": last_name}).delete().run(conn)
        if result["deleted"] == 0:
            return {"message": f"Заказчик {last_name} не найден"}
        return {"message": f"Заказчик {last_name} удалён"}
    except Exception as e:
        return {"error": f"Ошибка при удалении данных: {e}"}
    finally:
        conn.close()

#Очищает таблицу заказчиков
@app.delete("/clear/")
def clear_customers():
    conn = get_connection()
    if not conn:
        return {"error": "Не удалось подключиться к базе данных"}

    try:
        rdb.db(DB_NAME).table(TABLE_NAME).delete().run(conn)
        return {"message": "Таблица заказчиков очищена"}
    except Exception as e:
        return {"error": f"Ошибка при очистке таблицы: {e}"}
    finally:
        conn.close()

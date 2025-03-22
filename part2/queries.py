import rethinkdb as r
from tabulate import tabulate

rdb = r.RethinkDB()

# 4. Создать запросы для вывода:

# 4a) Все уникальные фамилии заказчиков и их скидки
def get_unique_customers(conn):
    return tabulate(list(rdb.db('rental_service').table('customers')
                .pluck('last_name', 'discount')
                .distinct()
                .run(conn)), headers="keys", tablefmt="grid")

# 4b) Все уникальные районы проживания заказчиков
def get_unique_districts(conn):
    return tabulate(list(rdb.db('rental_service').table('customers')
                .pluck('district')
                .distinct()
                .run(conn)), headers="keys", tablefmt="grid")

# 4c) Все названия пунктов проката и их расположение
def get_rental_points(conn):
    return tabulate(list(rdb.db('rental_service').table('rental_points')
                .pluck('number', 'location')
                .run(conn)), headers="keys", tablefmt="grid")


# 5. Запросы на выборку информации

# 5a) Заказчики из Приокского и Сормовского района или фамилии на "ин"
def get_filtered_customers(conn):
    return tabulate(list(rdb.db('rental_service').table('customers')
                .filter((rdb.row['district'].match("Приокский|Сормовский")) |
                        (rdb.row['last_name'].match(".*ин$")))
                .pluck('id', 'last_name')
                .run(conn)), headers="keys", tablefmt="grid")

# 5b) Записи проката с суммой > 2000 руб, сортировка по сумме и сроку
def get_expensive_rentals(conn):
    return tabulate(list(rdb.db('rental_service').table('rentals')
                .filter(rdb.row['amount'] > 2000)
                .order_by('amount', 'rental_period')
                .pluck('id', 'date', 'rental_period', 'amount')
                .run(conn)), headers="keys", tablefmt="grid")

# 5c) Вещи с количеством >= 7
def get_large_items(conn):
    return tabulate(list(rdb.db('rental_service').table('items')
                .filter(rdb.row['quantity'] >= 7)
                .pluck('name', 'storage')
                .run(conn)), headers="keys", tablefmt="grid")


# 6. Вывести данные о прокате вещей

# 6a) Фамилия клиента, название пункта проката, дата, номер прокатной квитанции (сортировка по фамилии и пункту)
def get_rental_details(conn):
    return tabulate(list(rdb.db('rental_service').table('rentals')
                .eq_join('customer_id', rdb.db('rental_service').table('customers'))
                .map(lambda row: {
                    "last_name": row['right']['last_name'],
                    "rental_point": row['left']['rental_point_id'],
                    "date": row['left']['date'],
                    "rental_id": row['left']['id']
                })
                .order_by('last_name', 'rental_point')
                .run(conn)), headers="keys", tablefmt="grid")

# 6b) Название пункта проката, дата, название вещи, сумма
def get_rental_summary(conn):
    return list(rdb.db('rental_service').table('rentals')
                .eq_join('rental_point_id', rdb.db('rental_service').table('rental_points'))
                .eq_join(lambda row: row['left']['item_id'], rdb.db('rental_service').table('items'))
                .map(lambda row: {
                    "rental_point": row['left']['right']['number'],
                    "date": row['left']['left']['date'],
                    "item_name": row['right']['name'],
                    "amount": row['left']['left']['amount']
                })
                .run(conn))


# 7. Вывести сложные запросы

# 7a) Названия прокатных пунктов, которые отдавали утюги или обслуживали клиентов своего района
def get_iron_rental_or_local_service(conn):
    return tabulate(list(
        rdb.db('rental_service').table('rentals')
        .eq_join('rental_point_id', rdb.db('rental_service').table('rental_points'))
        .map(lambda row: {
            "rental_point": row['right']['number'],
            "item_id": row['left']['item_id'],
            "rental_point_location": row['right']['location']
        })
        .eq_join('item_id', rdb.db('rental_service').table('items'))
        .filter(lambda row: (row['right']['name'] == "Утюг") |
                            (row['left']['rental_point_location'] == row['right']['storage']))
        .map(lambda row: {"rental_point": row['left']['rental_point']})  # Было неправильно
        .distinct()
        .run(conn)
    ), headers="keys", tablefmt="grid")


# 7b) Заказчики, взявшие в прокат вещи со стоимостью более 8000 руб не ранее февраля, с сортировкой по пунктам проката
def get_high_value_rentals(conn):
    return list(rdb.db('rental_service').table('rentals')
                .eq_join('customer_id', rdb.db('rental_service').table('customers'))
                .map(lambda row: {
                    "customer_name": row['right']['last_name'],
                    "customer_district": row['right']['district'],
                    "rental_point_id": row['left']['rental_point_id'],
                    "amount": row['left']['amount'],
                    "date": row['left']['date']
                })
                .filter(lambda row: (row['amount'] > 8000) & 
                                    (rdb.expr(["Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август"])
                                     .contains(row['date'])))
                .eq_join('rental_point_id', rdb.db('rental_service').table('rental_points'))
                .map(lambda row: {
                    "customer_name": row['left']['customer_name'],
                    "customer_district": row['left']['customer_district'],
                    "rental_point": row['right']['number']
                })
                .order_by('rental_point')
                .run(conn))



# 7c) Вещи, взятые заказчиком Кожаковым в пунктах других районов
def get_kojakov_rentals(conn):
    return list(rdb.db('rental_service').table('rentals')
                .eq_join('customer_id', rdb.db('rental_service').table('customers'))
                .map(lambda row: {
                    "customer_id": row['left']['customer_id'],
                    "customer_name": row['right']['last_name'],
                    "customer_district": row['right']['district'],
                    "rental_point_id": row['left']['rental_point_id'],
                    "item_id": row['left']['item_id']
                })
                .eq_join('rental_point_id', rdb.db('rental_service').table('rental_points'))
                .map(lambda row: {
                    "customer_name": row['left']['customer_name'],
                    "customer_district": row['left']['customer_district'],
                    "rental_point_location": row['right']['location'],
                    "item_id": row['left']['item_id']
                })
                .eq_join('item_id', rdb.db('rental_service').table('items'))
                .filter(lambda row: (row['left']['customer_name'] == "Кожаков") & 
                                    (row['left']['customer_district'] != row['left']['rental_point_location']))
                .map(lambda row: {
                    "item_name": row['right']['name'],
                    "rental_price": row['right']['rental_price']
                })
                .run(conn))



# 7d) Вещи, которые сдавались более чем в одном пункте проката
def get_multi_rental_items(conn):
    return list(rdb.db('rental_service').table('rentals')
                .group('item_id').count()
                .ungroup()
                .filter(lambda row: row['reduction'] > 1)
                .eq_join('group', rdb.db('rental_service').table('items'))
                .map(lambda row: {
                    "item_name": row['right']['name'],
                    "quantity_left": row['right']['quantity']
                })
                .run(conn))


# 10a) Найти вещи, бравшиеся в прокат заказчиками с размером скидки более 2%
def get_items_by_discount(conn):
    return list(
        rdb.db('rental_service').table('rentals')
        .filter(lambda rental:
            rdb.db('rental_service').table('customers')
            .filter(lambda customer: customer['discount'] > 2)
            .map(lambda customer: customer['id'])
            .contains(rental['customer_id'])
        )
        .eq_join('item_id', rdb.db('rental_service').table('items'))
        .map(lambda row: {
            "item_name": row['right']['name'],
            "rental_price": row['right']['rental_price']
        })
        .distinct()
        .run(conn)
    )


# 10b) Найти все вещи, бравшиеся в прокат заказчиком, бравшим что-либо в прокатных пунктах своего района
def get_items_by_local_rental(conn):
    local_customers = (
        rdb.db('rental_service').table('rentals')
        .eq_join('customer_id', rdb.db('rental_service').table('customers'))
        .eq_join(lambda row: row['left']['rental_point_id'], rdb.db('rental_service').table('rental_points'))
        .filter(lambda row: row['left']['left']['right']['district'] == row['right']['location'])
        .map(lambda row: row['left']['left']['customer_id'])
        .distinct()
        .coerce_to('array')
        .run(conn)
    )

    if not local_customers:
        return "Нет данных"

    return tabulate(list(
        rdb.db('rental_service').table('rentals')
        .filter(lambda rental: r.expr(local_customers).contains(rental['customer_id']))
        .eq_join('item_id', rdb.db('rental_service').table('items'))
        .map(lambda row: {
            "item_name": row['right']['name'],
            "rental_price": row['right']['rental_price']
        })
        .distinct()
        .run(conn)
    ), headers="keys", tablefmt="grid")


#11a) Определить те вещи, которые брались летом на самый продолжительный срок
def get_summer_longest_rentals(conn):
    summer_months = ["Июнь", "Июль", "Август"]

    max_rental_period = (
        rdb.db('rental_service').table('rentals')
        .filter(lambda rental: rdb.expr(summer_months).contains(rental['date']))
        .max('rental_period')['rental_period']
        .default(0)
        .run(conn)
    )

    if max_rental_period == 0:
        return []

    return list(
        rdb.db('rental_service').table('rentals')
        .filter(lambda rental: (rdb.expr(summer_months).contains(rental['date'])) & 
                               (rental['rental_period'] == max_rental_period))
        .eq_join('item_id', rdb.db('rental_service').table('items'))
        .map(lambda row: {
            "item_name": row['right']['name'],
            "rental_period": row['left']['rental_period']
        })
        .distinct()
        .run(conn)
    )
#11b) найти прокатные пункты, отдававшие вещи с самой большой ценой
def get_rental_points_max_price(conn):
    max_price = (
        rdb.db('rental_service').table('rentals')
        .max('amount')['amount']
        .default(0)
        .run(conn)
    )

    if max_price == 0:
        return []

    return list(
        rdb.db('rental_service').table('rentals')
        .filter(lambda rental: rental['amount'] == max_price)
        .eq_join('rental_point_id', rdb.db('rental_service').table('rental_points'))
        .map(lambda row: {
            "rental_point": row['right']['number'],
            "amount": row['left']['amount']
        })
        .distinct()
        .run(conn)
    )
#11c) найти таких заказчиков, которые имеют такой же размер скидки, как кто-либо из бравших на прокат радиоприемник
def get_customers_with_same_discount_as_radio_renters(conn):
    radio_discounts = (
        rdb.db('rental_service').table('rentals')
        .eq_join('item_id', rdb.db('rental_service').table('items'))
        .filter(lambda row: row['right']['name'] == "Радиоприемник")
        .eq_join(lambda row: row['left']['customer_id'], rdb.db('rental_service').table('customers'))
        .map(lambda row: row['right']['discount'])
        .distinct()
        .coerce_to('array')
        .run(conn)
    )

    if not radio_discounts:
        return []

    return list(
        rdb.db('rental_service').table('customers')
        .filter(lambda customer: rdb.expr(radio_discounts).contains(customer['discount']))
        .pluck('last_name', 'discount')
        .run(conn)
    )
#12) Используя операцию UNION получить адреса проживания заказчиков и места расположения прокатных пунктов
def get_addresses_union(conn):
    return list(
        rdb.db('rental_service').table('customers')
        .pluck({'district': True})
        .union(
            rdb.db('rental_service').table('rental_points')
            .pluck({'location': True})
        )
        .distinct()
        .run(conn)
    )
#13a) Найти две самые дорогие вещи, сдававшиеся в прокат не позднее октября
def get_two_most_expensive_items_before_october(conn):
    result = tabulate(list(
        rdb.db('rental_service').table('rentals')
        .filter(lambda rental: rdb.expr(["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь"])
                .contains(rental["date"]))
        .eq_join("item_id", rdb.db("rental_service").table("items"))
        .map(lambda row: {
            "item_name": row["right"]["name"],
            "rental_price": row["right"]["rental_price"]
        })
        .order_by(rdb.desc("rental_price"))
        .limit(2)
        .run(conn)
    ), headers="keys", tablefmt="grid")

    if not result:
        print("Нет данных! Добавьте дорогие вещи с датами до октября.")
    
    return result

#13b) Найти прокатные пункты, сдававшие все вещи всем заказчикам из Нижегородского района
def get_rental_points_with_all_items_for_nizhegorod_customers(conn):
    nizhegorod_customers = list(
        rdb.db('rental_service').table('customers')
        .filter({"district": "Нижегородский"})
        .map(lambda cust: cust["id"])
        .coerce_to("array")
        .run(conn)
    )

    if not nizhegorod_customers:
        return "⚠️ Нет заказчиков из Нижегородского района!"

    result = list(
        rdb.db('rental_service').table('rental_points')
        .filter(lambda rental_point:
            rdb.db('rental_service').table('rentals')
            .filter({"rental_point_id": rental_point["id"]})
            .filter(lambda rent: rdb.expr(nizhegorod_customers).contains(rent["customer_id"]))
            .count()
            .eq(rdb.db("rental_service").table("items").count())
        )
        .pluck("number", "location")
        .run(conn)
    )

    return tabulate(result, headers="keys", tablefmt="grid") if result else "⚠️ Нет подходящих пунктов."


#13c) найти заказчиков не бравших в прокат вещи ценой мене 5000руб. в прокатных пунктах чужих районов
def get_customers_not_renting_cheap_items_in_other_districts(conn):
    return tabulate(list(
        rdb.db('rental_service').table('customers')
        .filter(lambda customer:
            rdb.db('rental_service').table('rentals')
            .filter({"customer_id": customer["id"]})
            .eq_join("rental_point_id", rdb.db("rental_service").table("rental_points"))
            .eq_join(lambda rent: rent["left"]["item_id"], rdb.db("rental_service").table("items"))
            .filter(lambda rent: (rent["right"]["rental_price"] < 5000) &
                                 (rent["left"]["left"]["right"]["location"] != customer["district"]))
            .is_empty()
        )
        .pluck("id", "last_name", "district")
        .run(conn)
    ), headers="keys", tablefmt="grid")

#13d) найти заказчиков, бравших вещи во всех прокатных пунктах с размером комиссионных менее 5%
def get_customers_renting_in_all_low_commission_points(conn):
    low_commission_points = list(
        rdb.db("rental_service").table("rental_points")
        .filter(lambda point: point["commission"] < 5)
        .map(lambda point: point["id"])
        .coerce_to("array")
        .run(conn)
    )

    if not low_commission_points:
        return "⚠️ Нет пунктов проката с комиссией <5%."

    result = list(
        rdb.db("rental_service").table("customers")
        .filter(lambda customer:
            rdb.db("rental_service").table("rentals")
            .filter(lambda rental: rental["customer_id"] == customer["id"])
            .map(lambda rental: rental["rental_point_id"])
            .distinct()
            .coerce_to("array")
            .contains(*low_commission_points)
        )
        .pluck("last_name")
        .run(conn)
    )

    return tabulate(result, headers="keys", tablefmt="grid") if result else "⚠️ Никто не арендовал во всех пунктах."


#14a) найти средний срок проката вещей, бравшихся в прокатных пунктах Советского района
def get_avg_rental_period_in_sovetsky(conn):
    return rdb.db("rental_service").table("rentals") \
        .eq_join("rental_point_id", rdb.db("rental_service").table("rental_points")) \
        .filter(lambda row: row["right"]["location"] == "Советский") \
        .avg(lambda row: row["left"]["rental_period"]) \
        .run(conn)

#14b) найти заказчика, имеющего минимальную скидку среди бравших вещи в бюро проката N8
def get_customer_with_min_discount_in_n8(conn):
    customers_in_n8 = list(
        rdb.db("rental_service").table("rentals")
        .filter({"rental_point_id": "003"})
        .map(lambda rental: rental["customer_id"])
        .distinct()
        .coerce_to("array")
        .run(conn)
    )

    if not customers_in_n8:
        return "⚠️ Никто не арендовал в N8."

    result = list(
        rdb.db("rental_service").table("customers")
        .filter(lambda customer: rdb.expr(customers_in_n8).contains(customer["id"]))
        .order_by("discount")
        .limit(1)
        .pluck("last_name", "discount")
        .run(conn)
    )

    return tabulate(result, headers="keys", tablefmt="grid") if result else "⚠️ Нет данных."


#14c) найти те записи о прокате, где стоимость проката больше средней по району, в котором располагается бюро найма
def get_rentals_above_avg_by_district(conn):
    district_avg_prices = {
        entry["group"]: entry["reduction"] if entry["reduction"] is not None else 0
        for entry in rdb.db("rental_service").table("rentals")
        .eq_join("rental_point_id", rdb.db("rental_service").table("rental_points"))
        .group(rdb.row["right"]["location"])
        .avg(rdb.row["left"]["amount"])
        .ungroup()
        .run(conn)
    }

    result = list(
        rdb.db("rental_service").table("rentals")
        .eq_join("rental_point_id", rdb.db("rental_service").table("rental_points"))
        .map(lambda row: {
            "rental_id": row["left"]["id"],
            "rental_point": row["right"]["location"],
            "amount": row["left"]["amount"],
            "district": row["right"]["location"].default("")
        })
        .filter(lambda row:
            (row["district"] != "") &
            (rdb.expr(district_avg_prices).has_fields(row["district"])) &
            (row["amount"] > district_avg_prices[row["district"]])
        )
        .run(conn)
    )

    return tabulate(result, headers="keys", tablefmt="grid") if result else "⚠️ Нет прокатов дороже среднего."

#14d) найти общее число вещей, бравшихся Семеновым
def get_total_items_rented_by_semenov(conn):
    return rdb.db("rental_service").table("rentals") \
        .eq_join("customer_id", rdb.db("rental_service").table("customers")) \
        .filter(lambda row: row["right"]["last_name"] == "Семенов") \
        .count() \
        .run(conn)

#15a) найти суммарную величину стоимости проката для каждой вещи
def get_total_rental_amount_by_item(conn):
    return list(
        rdb.db("rental_service").table("rentals")
        .group("item_id")
        .sum("amount")
        .ungroup()
        .map(lambda row: {
            "item_id": row["group"],
            "total_rental_amount": row["reduction"]
        })
        .eq_join("item_id", rdb.db("rental_service").table("items"))
        .map(lambda row: {
            "item_name": row["right"]["name"],
            "total_rental_amount": row["left"]["total_rental_amount"]
        })
        .run(conn)
    )

#15b) определить для каждой вещи средний срок проката за осенний период
def get_avg_rental_period_in_autumn(conn):
    autumn_months = ["Сентябрь", "Октябрь", "Ноябрь"]

    return list(
        rdb.db("rental_service").table("rentals")
        .filter(lambda row: rdb.expr(autumn_months).contains(row["date"]))
        .group("item_id")
        .avg("rental_period")
        .ungroup()
        .eq_join("group", rdb.db("rental_service").table("items"))
        .map(lambda row: {
            "item_name": row["right"]["name"],
            "avg_rental_period": row["reduction"]
        })
        .run(conn)
    )

#15c) найти для каждого заказчика, бравшего вещи во всех бюро проката Советского района, число различных бравшихся в прокат вещей
def get_customers_with_all_sovetsky_rentals(conn):
    # Получаем список всех пунктов проката в Советском районе
    sovetsky_rental_points = list(
        rdb.db("rental_service").table("rental_points")
        .filter({"location": "Советский"})
        .map(lambda row: row["number"])
        .coerce_to("array")
        .run(conn)
    )

    return list(
        rdb.db("rental_service").table("rentals")
        .group("customer_id")
        .map(lambda row: row["rental_point_id"])
        .ungroup()
        .filter(lambda row: row["reduction"].set_difference(sovetsky_rental_points).count() == 0)
        .eq_join("group", rdb.db("rental_service").table("customers"))
        .map(lambda row: {
            "customer_name": row["right"]["last_name"],
            "items_count": rdb.db("rental_service").table("rentals")
            .filter({"customer_id": row["left"]})
            .count()
        })
        .run(conn)
    )

#15d) получить сводную таблицу “бюро проката - вещь-суммарная стоимость проката”
def get_rental_summary_by_point_and_item(conn):
    return list(
        rdb.db("rental_service").table("rentals")
        .group(rdb.row["rental_point_id"].add("-").add(rdb.row["item_id"]))
        .sum("amount")
        .ungroup()
        .map(lambda row: {
            "rental_point_item": row["group"],
            "total_rental_amount": row["reduction"]
        })
        .map(lambda row: {
            "rental_point_id": row["rental_point_item"].split("-")[0],
            "item_id": row["rental_point_item"].split("-")[1],
            "total_rental_amount": row["total_rental_amount"]
        })
        .eq_join("rental_point_id", rdb.db("rental_service").table("rental_points"))
        .map(lambda row: {
            "rental_point": row["right"]["number"],
            "item_id": row["left"]["item_id"],
            "total_rental_amount": row["left"]["total_rental_amount"]
        })
        .eq_join("item_id", rdb.db("rental_service").table("items"))
        .map(lambda row: {
            "rental_point": row["left"]["rental_point"],
            "item_name": row["right"]["name"],
            "total_rental_amount": row["left"]["total_rental_amount"]
        })
        .run(conn)
    )
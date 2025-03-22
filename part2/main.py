from rethinkdb import RethinkDB
from config import get_connection
from queries import (
    get_unique_customers, get_unique_districts, get_rental_points,
    get_filtered_customers, get_expensive_rentals, get_large_items,
    get_rental_details, get_rental_summary,
    get_iron_rental_or_local_service, get_high_value_rentals,
    get_kojakov_rentals, get_multi_rental_items,
    get_items_by_discount, get_items_by_local_rental, get_summer_longest_rentals, get_rental_points_max_price, get_customers_with_same_discount_as_radio_renters, get_addresses_union,
    get_two_most_expensive_items_before_october,
    get_rental_points_with_all_items_for_nizhegorod_customers,
    get_customers_not_renting_cheap_items_in_other_districts,
    get_customers_renting_in_all_low_commission_points, get_avg_rental_period_in_sovetsky,
    get_customer_with_min_discount_in_n8,
    get_rentals_above_avg_by_district,
    get_total_items_rented_by_semenov, get_total_rental_amount_by_item,
    get_avg_rental_period_in_autumn,
    get_customers_with_all_sovetsky_rentals,
    get_rental_summary_by_point_and_item
)

def main():
    conn = get_connection()

    while True:
        print("\nВыберите запрос:")
        print("1) - (4a) Уникальные заказчики")
        print("2) - (4b) Уникальные районы")
        print("3) - (4c) Пункты проката")
        print("4) - (5a) Фильтр заказчиков (Приокский, Сормовский, фамилия на 'ин')")
        print("5) - (5b) Дорогие прокаты (>2000 руб, сортировка)")
        print("6) - (5c) Вещи с количеством >= 7")
        print("7) - (6a) Детали проката (фамилия, пункт, дата, номер квитанции)")
        print("8) - (6d) Сводка проката (пункт, дата, вещь, сумма)")
        print("9) - (7a) Пункты с утюгами или локальным обслуживанием")
        print("10) - (7b) Дорогие заказы с февраля (>8000 руб)")
        print("11) - (7c) Вещи, взятые Кожаковым в других районах")
        print("12) - (7d) Вещи, сдаваемые более чем в 1 пункте проката")
        print("13) - (10a) Вещи, взятые заказчиками с скидкой >2%")
        print("14) - (10b) Вещи, взятые заказчиками, арендовавшими в пунктах своего района")
        print("15) - (11a) Вещи, которые брались летом на самый продолжительный срок")
        print("16) - (11b) Прокатные пункты, отдававшие вещи с самой большой ценой")
        print("17) - (11c) Заказчики, имеющие такой же размер скидки, как те, кто брал радиоприемник")
        print("18) - (12) Получить адреса проживания заказчиков и места расположения прокатных пунктов (UNION)")
        print("19) - (13a) Две самые дорогие вещи, сдававшиеся в прокат не позднее октября")
        print("20) - (13b) Прокатные пункты, сдававшие все вещи заказчикам из Нижегородского района")
        print("21) - (13c) Заказчики, не бравшие вещи дешевле 5000 руб. в чужих районах")
        print("22) - (13d) Заказчики, бравшие вещи во всех пунктах с комиссией <5%")
        print("23) - (14a) Средний срок проката в Советском районе")
        print("24) - (14b) Клиент с минимальной скидкой среди бравших в N8")
        print("25) - (14c) Прокаты, дороже среднего по району")
        print("26) - (14d) Общее число вещей, арендованных Семеновым")
        print("27) - (15a) Суммарная стоимость проката для каждой вещи")
        print("28) - (15b) Средний срок проката за осенний период")
        print("29) - (15c) Заказчики, бравшие во всех бюро Советского района")
        print("30) - (15d) Сводная таблица (бюро - вещь - сумма)")
        print("0) Выход")


        choice = input("\nВведите номер запроса: ")

        try:
            if choice == "1":
                print("Уникальные заказчики:", get_unique_customers(conn))
            elif choice == "2":
                print("Уникальные районы:", get_unique_districts(conn))
            elif choice == "3":
                print("Пункты проката:", get_rental_points(conn))
            elif choice == "4":
                print("Фильтр заказчиков:", get_filtered_customers(conn))
            elif choice == "5":
                print("Дорогие прокаты:", get_expensive_rentals(conn))
            elif choice == "6":
                print("Вещи с количеством >= 7:", get_large_items(conn))
            elif choice == "7":
                print("Детали проката:", get_rental_details(conn))
            elif choice == "8":
                print("Сводка проката:", get_rental_summary(conn))
            elif choice == "9":
                print("Пункты с утюгами или локальным обслуживанием:", get_iron_rental_or_local_service(conn))
            elif choice == "10":
                print("Дорогие заказы с февраля:", get_high_value_rentals(conn))
            elif choice == "11":
                print("Кожаков и аренда в других районах:", get_kojakov_rentals(conn))
            elif choice == "12":
                print("Вещи, сдаваемые более чем в 1 пункте проката:", get_multi_rental_items(conn))
            elif choice == "13":
                print("Вещи, взятые заказчиками с скидкой >2%:", get_items_by_discount(conn))
            elif choice == "14":
                print("Вещи, взятые заказчиками, арендовавшими в пунктах своего района:", get_items_by_local_rental(conn))
            elif choice == "15":
                print("Вещи с самым длинным летним прокатом:", get_summer_longest_rentals(conn))
            elif choice == "16":
                print("Пункты, отдававшие самые дорогие вещи:", get_rental_points_max_price(conn))
            elif choice == "17":
                print("Заказчики с такой же скидкой, как у бравших радиоприемник:", get_customers_with_same_discount_as_radio_renters(conn))
            elif choice == "18":
                print("Адреса заказчиков и пункты проката (UNION):", get_addresses_union(conn))
            elif choice == "19":
                print("Две самые дорогие вещи до октября:", get_two_most_expensive_items_before_october(conn))
            elif choice == "20":
                print("Прокатные пункты, сдававшие все вещи заказчикам из Нижегородского района:", get_rental_points_with_all_items_for_nizhegorod_customers(conn))
            elif choice == "21":
                print("Заказчики, не бравшие дешёвые вещи в чужих районах:", get_customers_not_renting_cheap_items_in_other_districts(conn))
            elif choice == "22":
                print("Заказчики, бравшие вещи во всех пунктах с комиссией <5%:", get_customers_renting_in_all_low_commission_points(conn))
            elif choice == "23":
                print("Средний срок проката в Советском районе:", get_avg_rental_period_in_sovetsky(conn))
            elif choice == "24":
                print("Клиент с минимальной скидкой среди бравших в N8:", get_customer_with_min_discount_in_n8(conn))
            elif choice == "25":
                print("Прокаты, дороже среднего по району:", get_rentals_above_avg_by_district(conn))
            elif choice == "26":
                print("Общее число вещей, арендованных Семеновым:", get_total_items_rented_by_semenov(conn))
            elif choice == "27":
                print("Суммарная стоимость проката для каждой вещи:", get_total_rental_amount_by_item(conn))
            elif choice == "28":
                print("Средний срок проката за осенний период:", get_avg_rental_period_in_autumn(conn))
            elif choice == "29":
                print("Заказчики, бравшие во всех бюро Советского района:", get_customers_with_all_sovetsky_rentals(conn))
            elif choice == "30":
                print("Сводная таблица (бюро - вещь - сумма):", get_rental_summary_by_point_and_item(conn))
            elif choice == "0":
                print("Выход.")
                break
            else:
                print("Некорректный ввод, попробуйте снова.")

        except Exception as e:
            print(f"Ошибка: {e}")

    conn.close()
    print("Соединение с базой данных закрыто.")

if __name__ == "__main__":
    main()

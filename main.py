from random import randint as rdt
from random import randrange as rdg
from datetime import datetime as dt
from pprint import pprint
import psycopg2

# bd_password = input("Введите пароль от Базы Данных: ")

connection = psycopg2.connect(
    dbname='markets',
    user='nurtai',
    password='5432',
    host='localhost'
)

cursor = connection.cursor()


# 1. Узнайте какие телефоны из Kivano стоят столько же сколько и компьютеры из Sulpak.
# Solution 1:
# SELECT kivano.price,kivano.product_name, sulpak.price, sulpak.product_name
    # FROM kivano,sulpak
        # WHERE kivano.category_id = 1 AND sulpak.category_id = 2 AND kivano.price = sulpak.price;
# cursor.execute('SELECT * FROM sulpak;')

# 2. Узнайте самую последнюю модель Iphone в магазинах.
# cursor.execute('''SELECT product_name, price FROM kivano WHERE product_name LIKE '%12' ;''')

# 3. Выведите на экран список всех ноутбуков
# из sulpak и только тех телефонов которые имеют одинаковую дату выхода с компьютером из таблицы kivano.
# cursor.execute('''SELECT kivano.product_name, produsers.prodused_date
# FROM kivano
# INNER JOIN produsers ON kivano.item_id = produsers.produser_id
# WHERE kivano.category_id = 1
# GROUP BY kivano.product_name, produsers.prodused_date, kivano.category_id; ''')

# 5. Напишите запрос, который выводит продукты любого магазина в порядке их добавления.
# cursor.execute('''SELECT * FROM kivano, sulpak GROUP BY kivano.produser_id, sulpak.produser_id
#                 ORDER BY kivano.produser_id, sulpak.produser_id;''')

# 6. Найдите товары, которые есть в kivano но нет в sulpak.
# cursor.execute('''SELECT kivano.product_name, kivano.price FROM kivano LEFT JOIN sulpak
#                ON kivano.product_name=sulpak.product_name GROUP BY kivano.product_name, kivano.price;''')

# 7. Найдите все товары в магазине sulpak, где компания-производитель содержит букву "m" в имени.
# cursor.execute('''SELECT sulpak.produser_id, produsers.produser_company
#                FROM sulpak, produsers WHERE produsers.produser_company LIKE '%m%'
#                GROUP BY sulpak.produser_id, produsers.produser_company;''')

# 8. Найдите товары, которые есть и в kivano u sulpak.
# cursor.execute('''SELECT kivano.product_name FROM kivano
#                 INNER JOIN sulpak ON kivano.product_name=sulpak.product_name
#                 GROUP BY kivano.product_name;''')


# 12. Найдите самый последний японский товар который был добавлен в produsers.
# cursor.execute('''SELECT * FROM public.produsers WHERE produser_country='Japan' order by prodused_date DESC limit 1;''')

# 18. Acer закрыл свою фабрику в Бразилии после 2012
# года и переехал в Германию, у всех записей в produsers
# где Acer был произведен в Brazil после 2012 поставьте Germany.
# cursor.execute('''UPDATE produsers
#                 SET produser_country='Germany' WHERE prodused_date>'2011-12-31'
#                 AND produser_company='Aser' AND produser_country='Brazil'
# ;''')
# cursor.execute('''SELECT * FROM produsers WHERE produser_company='Aser';''')

# 25. В producers поменяйте Nokia на Microsoft везде где у компании Nokia указана страна USA.
# cursor.execute('''UPDATE produsers
#                  SET produser_company='Microsoft' WHERE
#                  produser_company='Nokia' AND produser_country='USA'
#  ;''')
# cursor.execute('''SELECT * FROM produsers WHERE produser_company='Microsoft';''')

# 9. Найдите китайские товары из kivano, которые в названии содержат "k".
# cursor.execute('''select * from public.kivano where produser_id = 21 and product_name like '%k%'
# or produser_id = 30 and product_name like '%k%' or produser_id = 52 and product_name like '%k%'
# or produser_id = 54 and product_name like '%k%' or produser_id = 58 and product_name like '%k%'
# or produser_id = 62 and product_name like '%k%' or produser_id = 65 and product_name like '%k%'
# or produser_id = 67 and product_name like '%k%' or produser_id = 70 and product_name like '%k%'
# or produser_id = 76 and product_name like '%k%' or produser_id = 80 and product_name like '%k%'
# or produser_id = 89 and product_name like '%k%';''')

# 10. Найдите самый последний добавленный товар в таблице produsers, и поменяйте компанию на Apple, и страну на kyrgyzstan.
# cursor.execute('''UPDATE public.produsers SET produser_country = 'Kyrgyzstan', produser_company = 'Apple'
# WHERE prodused_date = '2023-01-12';''')
# cursor.execute('''SELECT * FROM public.produsers order by prodused_date DESC;''')

# 11. Нужно объеденить 2 магазина по product_name и вывести на экран имя продукта и его цену из обоих магазинов,
# однако не факт что в обоих магазинах будут одинаковые товары, поэтому нужно joinить по полной.
# cursor.execute('''SELECT * FROM public.kivano FULL OUTER JOIN public.sulpak on kivano.product_name=sulpak.product_name;''')

# 12. Найдите самый последний японский товар который был добавлен в produsers.
# cursor.execute('''SELECT * FROM public.produsers WHERE produser_country = 'Japan' order by prodused_date DESC limit 1;''')

# 13. Напишите запрос, который прибавит 1000 к цене всех продуктов в sulpak.
# cursor.execute(''' UPDATE public.sulpak SET price = price + 1000;''')

# 14. Узнать разницу между самой высокой ценой в sulpak и самой низкой ценой товар в kivano.
# cursor.execute('''SELECT(SELECT MAX(price) FROM public.sulpak) - (select min(price) FROM public.sulpak);''')

# 15. Выведите на экран цены самых дешёвых телефонов из обоих магазинов.
# cursor.execute('''SELECT (SELECT MIN(price) FROM public.kivano where category_id = 1),
# (SELECT MIN(price) FROM public.sulpak WHERE category_id = 1);''')

# # 21. Найдите товары в sulpak, цена которых больше среднего на 2000 и меньше средний на 2000
# cursor.execute('''SELECT * FROM public.sulpak WHERE price > (SELECT avg(price) + 2000 FROM public.sulpak)
# OR price < (SELECT avg(price) - 2000 FROM public.sulpak);''')



pprint(cursor.fetchall())
connection.commit()
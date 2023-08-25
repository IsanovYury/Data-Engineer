/* 
SQL-запрос на формирование отчета по ежедневным продажам
*/ 
WITH cat AS 
( -- id только товаров
SELECT distinct
    to_base64(c1._idrref) cat_1,
    to_base64(c2._idrref) cat_2,
    to_base64(c3._idrref) cat_3
FROM 
    schema._reference141 c -- таблица номенклатурных групп
LEFT JOIN 
    schema._reference141 c1 ON c1._parentidrref = c._idrref
LEFT JOIN 
    schema._reference141 c2 ON c2._parentidrref = COALESCE(c1._idrref, c._idrref)
LEFT JOIN 
    schema._reference141 c3 ON c3._parentidrref = COALESCE(c2._idrref, c1._idrref, c._idrref)
WHERE 
    to_base64(c._parentidrref) = 'AAAAAAAAAAAAAAAAAAAAAA==' AND  c._description = 'Товары'
)  
,
/* 
Подзапрос на формирование чеков
*/ 
sales as (SELECT 
    i.item_code as webcode,
    i._description as item_name,
    replace(store.store, 'Магазин ', '') as store,
    sum(CAST(g.kf_quan AS INT)) quantity,
    sum(CAST(g.kf_curr AS INT)) item_total,
    store.city,
    i.cat_id
FROM 
    schema.general g
LEFT JOIN
    (SELECT DISTINCT to_base64(_idrref) item, _description, CAST(CAST(_fld26454 AS INT) AS varchar) item_code,
    to_base64(_fld2428rref) cat_id FROM  schema._reference140) i ON g.item = i.item  -- таблица номенклатур
LEFT JOIN
    (select store_id, store_code, store,  city, country from schema.stores) store ON g.shops = store.store_id
WHERE
    g.post = 'AQ==' AND                       -- признак проводки
    store.country = 'KZ' AND                   -- признак страны
    g."delete" != 'AQ==' AND                  -- признак удаленного документа
    g.b2bflag != 'AQ==' AND                   -- признак документа b2b
    g."date" = date('{{ ds }}') AND           -- признак даты чека
    g.item IS NOT NULL AND
    g.opertype = 'kE3odS6MJUhDNPleoFllIg=='   -- признак продажи без возврата
GROUP BY store.store, i.item_code, i._description, store.country, store.city, i.cat_id,g.whs,g.whs_i)
/* 
Итоговый отчет
*/ 
SELECT 
    sales.webcode,
    sales.item_name,
    sales.store,
    sales.quantity,
    sales.item_total,
    sales.city
FROM sales
INNER JOIN cat ON sales.cat_id =  COALESCE(cat.cat_3, cat.cat_2, cat.cat_1)  -- товары с категориями
WHERE COALESCE(cat.cat_3, cat.cat_2, cat.cat_1) NOT IN ('AccComp Программное обеспечение',  'AccComp Медиа Сервисы')  
   AND sales.webcode NOT IN ('216158', '62763', '108285')                    -- отсекаем пакеты(категория accessories)

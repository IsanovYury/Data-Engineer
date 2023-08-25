/* 
SQL-запрос на формирование магазинов, ПВЗ, складов
*/ 
WITH stores AS (
SELECT 
    store.store_id,
    replace(store.store, 'Магазин ', '') AS store,
    store.city,
    to_base64(whs._idrref) AS whs_id
FROM schema._reference188 whs   -- таблица складов
LEFT JOIN (SELECT store_id, store, city, country FROM schema.stores) store
ON store.store_id = to_base64(whs._fld30704rref)
WHERE 
    store.country = 'KZ' AND store.city != 'Бишкек' AND    -- признак страны
    to_base64(whs._fld25076) = 'AQ=='                      -- признак доступности к покупке
)
/* 
SQL-запрос на формирование стоков по магазинам, складам и ПВЗ
*/ 
SELECT  
    stock.items_id, 
    sum(stock.quant) AS stock, 
    sum(stock.price) AS price,
    stores.store, 
    stores.city
FROM schema.stock_daily stock
INNER JOIN stores
    on stock.warehouse_id = stores.whs_id
WHERE 
    cast(stock."date" AS date) = cast('{{ data_interval_start.date() }}' AS date) AND 
    stock.country = 'KZ' AND
    (stock.quality = 'N' OR stock.quality = '') -- товары новые и без брака
GROUP BY 1,4,5

    
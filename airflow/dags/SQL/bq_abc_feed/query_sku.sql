/* 
SQL-запрос на формирование sku категориями (в т.ч. с родительскими)
*/ 
with prod_cat as 
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
    to_base64(c._parentidrref) = 'AAAAAAAAAAAAAAAAAAAAAA==' AND c._description = 'Товары' 
) 
/* 
SQL-запрос на формирование sku с категориями
*/ 
SELECT DISTINCT 
    CASE 
        WHEN length(cat.directortxt) > 0 THEN cat.directortxt
        ELSE 'Не присвоено'
    END AS category_short,
    prod._description AS item_name,
    to_base64(prod._idrref) items_id,
    CAST(CAST(prod._fld26454 AS INT) AS varchar) webcode,
    cat.categtxt AS category_name
FROM  
    schema._reference140 prod   -- таблица номенклатур
LEFT JOIN 
    (SELECT distinct zitemsn_n_wc as webcode, directortxt, categtxt, categ_id FROM schema.zitemsn) cat ---таблица sku и категорий
    ON CAST(CAST(prod._fld26454 AS decimal) AS varchar) = CAST(CAST(cat.webcode AS decimal) AS varchar)
INNER JOIN prod_cat
    ON (cat.categ_id = prod_cat.cat_1 OR cat.categ_id = prod_cat.cat_2 OR cat.categ_id = prod_cat.cat_3)

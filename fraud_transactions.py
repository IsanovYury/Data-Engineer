# Ежедневный код

#

# Коннект
import psycopg2 as ps
import pandas as pd

conn_src = ps.connect (database = 'bank', 
                       host = '***', 
                       user = '***', 
                       password = '***', 
                       port = '***')
conn_tgt = ps.connect(
                        database= 'edu',
                        host = '***',
                        user= '***',
                        password= '***',
                        port=  '***'
)

conn_src.autocommit = False
conn_tgt.autocommit = False

curs_src = conn_src.cursor()
curs_tgt = conn_tgt.cursor()


# 1.1 Работа с таблицей clients

# Очистка стейджинга

curs_tgt.execute("""delete from de11tm.isan_stg_clients""")
curs_tgt.execute("""delete from de11tm.isan_stg_clients_1""")
curs_tgt.execute("""delete from de11tm.isan_stg_clients_del""")

# забираем из таблицы-источника новые записи

curs_src.execute("""select 
                        client_id,
                        last_name,
                        first_name,
                        patronymic,
                        date_of_birth,
                        passport_num,
                        passport_valid_to,
                        phone,
                        create_dt as effective_from
                    from info.clients
                    """)

res = curs_src.fetchall()
names = [name[0] for name in curs_src.description]

curs_src.description
df = pd.DataFrame(res, columns = names)

curs_tgt.executemany("""insert into de11tm.isan_stg_clients_1
                                    (
                                        client_id,
                                        last_name,
                                        first_name,
                                        patronymic,
                                        date_of_birth,
                                        passport_num,
                                        passport_valid_to,
                                        phone,
                                        update_dt
                                      )
                        VALUES (%s, %s ,%s, %s, %s, %s, %s, %s, %s)""", df.values.tolist())



curs_tgt.execute ("""insert into de11tm.isan_stg_clients
                                    (
                                        client_id,
                                        last_name,
                                        first_name,
                                        patronymic,
                                        date_of_birth,
                                        passport_num,
                                        passport_valid_to,
                                        phone,
                                        update_dt
                                      )
                      select
                                        client_id,
                                        last_name,
                                        first_name,
                                        patronymic,
                                        date_of_birth,
                                        passport_num,
                                        passport_valid_to,
                                        phone,
                                        update_dt
                      from de11tm.isan_stg_clients_1
                      where update_dt > (select max_update_dt from de11tm.isan_meta_clients)""")


# Обрабатываем insert, update и delete в приемнике
# обрабатываем в таблице-приемнике записи, которые были добавлены на источнике 
# обработка insert	

curs_tgt.execute("""insert into de11tm.isan_dwh_dim_clients_hist
                                      (
                                      client_id,
                                      last_name,
                                      first_name,
                                      patronymic,
                                      date_of_birth,
                                      passport_num,
	                              passport_valid_to,
                                      phone,
                                      effective_from,
                                      effective_to,
                                      deleted_flg
                                      )
                        select        stg.client_id,
		                      stg.last_name,
		                      stg.first_name,
		                      stg.patronymic,
		                      stg.date_of_birth,
		                      stg.passport_num,
		                      stg.passport_valid_to,
		                      stg.phone,
		                      stg.update_dt as effective_from_dttm,
                                      cast('5999-12-31' as timestamp) as effective_to_dttm,
                                      '0' as DELETED_FLG 
                        from de11tm.isan_stg_clients stg
                            left join de11tm.isan_dwh_dim_clients_hist tgt
                            on stg.client_id = tgt.client_id
                            and tgt.effective_to = cast('5999-12-31' as timestamp)
                            and tgt.deleted_flg = 0
                        where tgt.client_id is NULL""")

#обрабатываем в таблице-приемнике записи, которые были обновлены на источнике
# обработка update

curs_tgt.execute( """update de11tm.isan_dwh_dim_clients_hist targ
                        set
                        effective_to = tmp.update_dt - interval '1 second'
                      from (select 
                                stg.client_id,
                                stg.update_dt
                            from de11tm.isan_stg_clients stg
                                inner join de11tm.isan_dwh_dim_clients_hist tgt
                                on stg.client_id = tgt.client_id
                                and tgt.effective_to = cast('5999-12-31' as timestamp)
                                and tgt.deleted_flg = 0
                            where (stg.passport_num != tgt.passport_num)
                                  or (stg.phone != stg.phone)) tmp
                        where targ.client_id = tmp.client_id
                              and targ.effective_to = cast('5999-12-31' as timestamp)
                              and targ.deleted_flg = 0
                              """)

curs_tgt.execute( """insert into de11tm.isan_dwh_dim_clients_hist
                          select      stg.client_id,
		                      stg.last_name,
		                      stg.first_name,
		                      stg.patronymic,
		                      stg.date_of_birth,
		                      stg.passport_num,
		                      stg.passport_valid_to,
	                              stg.phone,
		                      stg.update_dt as effective_from,
                                      cast('5999-12-31' as timestamp) as effective_to,
                                      '0' as DELETED_FLG 
                          from de11tm.isan_stg_clients stg
                                      inner join de11tm.isan_dwh_dim_clients_hist tgt
                                      on stg.client_id = tgt.client_id
                                      and tgt.effective_to = stg.update_dt - interval '1 second'
                                      and tgt.deleted_flg = 0
                          """
)

#обрабатываем в таблице-приемнике записи, которые были удалены на источнике
#обработка delete

curs_tgt.execute( """insert into de11tm.isan_dwh_dim_clients_hist 
                        select        tgt.client_id,
	                              tgt.last_name,
		                      tgt.first_name,
		                      tgt.patronymic,
		                      tgt.date_of_birth,
		                      tgt.passport_num,
	                              tgt.passport_valid_to,
		                      tgt.phone,
		                      now() as effective_from,
		                      cast('5999-12-31' as timestamp) as effective_to,
	                              1 as deleted_flg
                        from de11tm.isan_dwh_dim_clients_hist tgt
                              left join de11tm.isan_stg_clients stg
                              on tgt.client_id = stg.client_id
                        where tgt.effective_to = cast('5999-12-31' as timestamp)
                              and tgt.deleted_flg = 0
                              and stg.client_id is null""")

curs_tgt.execute( """update de11tm.isan_dwh_dim_clients_hist targ
                        set 
                        effective_to = now()- interval '1 second'
                      from (select tgt.client_id
                            from de11tm.isan_dwh_dim_clients_hist  tgt
                              left join de11tm.isan_stg_clients stg
                              on tgt.client_id = stg.client_id
                            where
                              tgt.effective_to = cast('5999-12-31' as timestamp)
                              and tgt.deleted_flg = 0
                              and stg.client_id is null) tmp
                      where targ.client_id = tmp.client_id
                            and targ.effective_to = cast('5999-12-31' as timestamp)
                            and targ.deleted_flg = 0
                        """)
# Обновляем метаданные

curs_tgt.execute("""delete from de11tm.isan_meta_clients""")

curs_tgt.execute("""insert into de11tm.isan_meta_clients (max_update_dt)
                    Select 
                        max(update_dt) as max_dt
                    from de11tm.isan_stg_clients""")




# 1.2 Работа с таблицей accounts

# Очистка стейджинга

curs_tgt.execute("""delete from de11tm.isan_stg_accounts""")
curs_tgt.execute("""delete from de11tm.isan_stg_accounts_1""")
curs_tgt.execute("""delete from de11tm.isan_stg_accounts_del""")

# забираем из таблицы-источника новые записи

#insert в stg слой
curs_src.execute("""select 
                        account,
                        valid_to,
                        client,
                        create_dt,
                        update_dt
                    from info.accounts
                    """)
res = curs_src.fetchall()
names = [name[0] for name in curs_src.description]

curs_src.description
df = pd.DataFrame(res, columns = names)

curs_tgt.executemany("""insert into de11tm.isan_stg_accounts_1
                                    (
                                     account_num,
		                     valid_to,
	                             client,
		                     create_dt,
		                     update_dt
                                    )
                        VALUES (%s, %s ,%s, %s, %s)
                        """, df.values.tolist())

curs_tgt.execute ("""insert into de11tm.isan_stg_accounts
                                    (
                                     account_num,
		                     valid_to,
	                             client,
		                     create_dt,
		                     update_dt
                                      )
                      select
                                     account_num,
		                     valid_to,
	                             client,
		                     create_dt,
		                     update_dt
                      from de11tm.isan_stg_accounts_1
                      where (create_dt > (select max_update_dt from de11tm.isan_meta_accounts))
                      or ( update_dt is not null and
                          update_dt > (select max_update_dt from de11tm.isan_meta_accounts))""")
curs_tgt.execute("""delete from de11tm.isan_stg_accounts_1""")


# Обрабатываем insert, update и delete в приемнике
# обрабатываем в таблице-приемнике записи, которые были добавлены на источнике 
# обработка insert	

curs_tgt.execute("""insert into de11tm.isan_dwh_dim_accounts_hist
                                     (
                                     account_num,
	                             valid_to,
	                             client, 
		                     effective_from,
		                     effective_to, 
		                     deleted_flg 
                                     )
                        select       stg.account_num,
		                     stg.valid_to,
	                             stg.client,
		                     stg.create_dt,
		                     coalesce (stg.update_dt, '5999-12-31'), 
                                     0 as DELETED_FLG 
                        from de11tm.isan_stg_accounts stg
                            left join de11tm.isan_dwh_dim_accounts_hist tgt
                            on stg.account_num = tgt.account_num
                            and tgt.effective_to = cast('5999-12-31' as timestamp)
                            and tgt.deleted_flg = 0
                        where tgt.account_num is NULL""")

#обрабатываем в таблице-приемнике записи, которые были обновлены на источнике
# обработка update

curs_tgt.execute( """update de11tm.isan_dwh_dim_accounts_hist targ
                        set
                        effective_to = tmp.update_dt - interval '1 second'
                      from (select 
                                stg.account_num,
                                stg.update_dt
                            from de11tm.isan_stg_accounts stg
                                inner join de11tm.isan_dwh_dim_accounts_hist tgt
                                on stg.account_num = tgt.account_num
                                and tgt.effective_to = cast('5999-12-31' as timestamp)
                                and tgt.deleted_flg = 0
                            where stg.client != tgt.client) tmp
                        where targ.account_num = tmp.account_num
                              and targ.effective_to = cast('5999-12-31' as timestamp)
                              and targ.deleted_flg = 0
                              """)

curs_tgt.execute( """insert into de11tm.isan_dwh_dim_accounts_hist
                          select  stg.account_num,
		                  stg.valid_to,
	                          stg.client,
		                  stg.create_dt,
		                  coalesce (stg.update_dt, '5999-12-31'), 
                                  0 as DELETED_FLG 
                          from de11tm.isan_stg_accounts stg
                                  inner join de11tm.isan_dwh_dim_accounts_hist tgt
                                  on stg.account_num = tgt.account_num
                                  and tgt.effective_to = stg.update_dt - interval '1 second'
                                  and tgt.deleted_flg = 0
                          """
)

#обрабатываем в таблице-приемнике записи, которые были удалены на источнике
#обработка delete

curs_tgt.execute( """insert into de11tm.isan_dwh_dim_accounts_hist 
                        select  stg.account_num,
		                stg.valid_to,
	                        stg.client,
		                stg.create_dt,
		                coalesce (stg.update_dt, '5999-12-31'), 
                                0 as DELETED_FLG 
                        from de11tm.isan_dwh_dim_accounts_hist tgt
                              left join de11tm.isan_stg_accounts stg
                              on tgt.account_num = stg.account_num
                        where tgt.effective_to = cast('5999-12-31' as timestamp)
                              and tgt.deleted_flg = 0
                              and stg.account_num is null""")

curs_tgt.execute( """update de11tm.isan_dwh_dim_accounts_hist targ
                        set 
                        effective_to = now()- interval '1 second'
                      from (select tgt.account_num
                            from de11tm.isan_dwh_dim_accounts_hist  tgt
                              left join de11tm.isan_stg_accounts stg
                              on tgt.account_num = stg.account_num
                            where
                              tgt.effective_to = cast('5999-12-31' as timestamp)
                              and tgt.deleted_flg = 0
                              and stg.account_num is null) tmp
                      where targ.account_num = tmp.account_num
                            and targ.effective_to = cast('5999-12-31' as timestamp)
                            and targ.deleted_flg = 0
                        """)

# Обновляем метаданные

curs_tgt.execute("""delete from de11tm.isan_meta_accounts""")

curs_tgt.execute("""insert into de11tm.isan_meta_accounts (max_update_dt)
                    Select 
                        max(create_dt) as max_dt
                    from de11tm.isan_stg_accounts""")




# 1.3 Работаем с таблицей cards

# Очистка стейджинга

curs_tgt.execute("""delete from de11tm.isan_stg_cards""")
curs_tgt.execute("""delete from de11tm.isan_stg_cards_1""")
curs_tgt.execute("""delete from de11tm.isan_stg_cards_del""")

# забираем из таблицы-источника новые записи

#insert в stg слой
#insert в stg слой
curs_src.execute("""select 
                        card_num,
                        account,
                        create_dt,
                        update_dt
                    from info.cards
                    """)
res = curs_src.fetchall()
names = [name[0] for name in curs_src.description]

curs_src.description
df = pd.DataFrame(res, columns = names)

curs_tgt.executemany("""insert into de11tm.isan_stg_cards_1
                                    (
                                     card_num,
			             account_num,
                                     create_dt,
                                     update_dt
                                      )
                        VALUES (%s, %s ,%s, %s)""", df.values.tolist())


curs_tgt.execute ("""insert into de11tm.isan_stg_cards
                                    (
                                     card_num, 
			             account_num,
                                     create_dt,
                                     update_dt
                                      )
                      select
                                     replace ( card_num , ' ', ''), 
			             account_num,
                                     create_dt,
                                     update_dt
                      from de11tm.isan_stg_cards_1
                      where (create_dt > (select max_update_dt from de11tm.isan_meta_cards))
                      or ( update_dt is not null and
                          update_dt > (select max_update_dt from de11tm.isan_meta_cards))""")
curs_tgt.execute("""delete from de11tm.isan_stg_accounts_1""")


# Обрабатываем insert, update и delete в приемнике
# обрабатываем в таблице-приемнике записи, которые были добавлены на источнике 
# обработка insert	

curs_tgt.execute("""insert into de11tm.isan_dwh_dim_cards_hist
                            (
                            card_num,
		            account_num,
		            effective_from,
		            effective_to, 
		            deleted_flg
                            )
                        select       stg.card_num,
		                     stg.account_num,
		                     stg.create_dt,
		                     coalesce (stg.update_dt, '5999-12-31'), 
                                     0 as DELETED_FLG 
                        from de11tm.isan_stg_cards stg
                            left join de11tm.isan_dwh_dim_cards_hist tgt
                            on stg.card_num = tgt.card_num
                            and tgt.effective_to = cast('5999-12-31' as timestamp)
                            and tgt.deleted_flg = 0
                        where tgt.card_num is NULL""")

#обрабатываем в таблице-приемнике записи, которые были обновлены на источнике
# обработка update

curs_tgt.execute( """update de11tm.isan_dwh_dim_cards_hist targ
                        set
                        effective_to = tmp.update_dt - interval '1 second'
                      from (select 
                                stg.card_num,
                                stg.update_dt
                            from de11tm.isan_stg_cards stg
                                inner join de11tm.isan_dwh_dim_cards_hist tgt
                                on stg.card_num = tgt.card_num
                                and tgt.effective_to = cast('5999-12-31' as timestamp)
                                and tgt.deleted_flg = 0
                            where stg.account_num != tgt.account_num) tmp
                        where targ.card_num = tmp.card_num
                              and targ.effective_to = cast('5999-12-31' as timestamp)
                              and targ.deleted_flg = 0
                              """)

curs_tgt.execute( """insert into de11tm.isan_dwh_dim_cards_hist
                          select  stg.card_num,
	                                stg.account_num,
		                              stg.create_dt,
		                              coalesce (stg.update_dt, '5999-12-31'), 
                                  0 as DELETED_FLG 
                          from de11tm.isan_stg_cards stg
                                  inner join de11tm.isan_dwh_dim_cards_hist tgt
                                  on stg.card_num = tgt.card_num
                                  and tgt.effective_to = stg.update_dt - interval '1 second'
                                  and tgt.deleted_flg = 0
                          """
)

#обрабатываем в таблице-приемнике записи, которые были удалены на источнике
#обработка delete

curs_tgt.execute( """insert into de11tm.isan_dwh_dim_cards_hist 
                        select  stg.card_num,
	                        stg.account_num,
		                stg.create_dt,
		                coalesce (stg.update_dt, '5999-12-31'), 
                                0 as DELETED_FLG 
                        from de11tm.isan_dwh_dim_cards_hist tgt
                              left join de11tm.isan_stg_cards stg
                              on tgt.card_num = stg.card_num
                        where tgt.effective_to = cast('5999-12-31' as timestamp)
                              and tgt.deleted_flg = 0
                              and stg.card_num is null""")

curs_tgt.execute( """update de11tm.isan_dwh_dim_cards_hist targ
                        set 
                        effective_to = now()- interval '1 second'
                      from (select tgt.card_num
                            from de11tm.isan_dwh_dim_cards_hist  tgt
                              left join de11tm.isan_stg_cards stg
                              on tgt.card_num = stg.card_num
                            where
                              tgt.effective_to = cast('5999-12-31' as timestamp)
                              and tgt.deleted_flg = 0
                              and stg.card_num is null) tmp
                      where targ.card_num = tmp.card_num
                            and targ.effective_to = cast('5999-12-31' as timestamp)
                            and targ.deleted_flg = 0
                        """)

# Обновляем метаданные

curs_tgt.execute("""delete from de11tm.isan_meta_cards""")

curs_tgt.execute("""insert into de11tm.isan_meta_cards (max_update_dt)
                    Select 
                        max(create_dt) as max_dt
                    from de11tm.isan_stg_cards""")





# Находим переменную-дату

from datetime import date
cur_dt = date.today()
if len(str(cur_dt.day))==1:
    cur_day = '0'+str(cur_dt.day)
else:
    cur_day = str (cur_dt.day)

if len(str(cur_dt.month))==1:
    cur_month = '0' + str(cur_dt.month)
else:
    cur_month = str(cur_dt.month)

current_dt = cur_day + cur_month + str(cur_dt.year)




# 1.4 Работаем с таблицей transactions

# Очищаем stg слой
curs_tgt.execute ("""Delete from de11tm.isan_stg_transactions""")

# загружаем данные в stg- слой

df = pd.read_csv(f'/home/de11tm/isan/project/transactions_{+current_dt}.csv',delimiter = ';')

curs_tgt.executemany("""insert into de11tm.isan_stg_transactions
                        (trans_id, trans_dt, amt, card_num, oper_type, oper_result, terminal)
                        values (%s, %s,%s,%s,%s,%s,%s)""",  df.values.tolist())

# Обрабатываем insert, update и delete в приемнике
# обрабатываем в таблице-приемнике записи, которые были добавлены на источнике 
# обработка insert	

curs_tgt.execute("""insert into de11tm.isan_dwh_fact_transactions
                        (trans_id,
		         trans_date,
		         card_num,
		         oper_type,
	                 amt,
	                 oper_result,
	                 terminal)
                    select 
                        replace (trans_id, ' ', ''),
		        trans_dt,
		        replace (card_num, ' ', ''),
		        oper_type,
	                amt,
	                oper_result,
	                terminal
                    from de11tm.isan_stg_transactions
                    where trans_dt > (select max_update_dt from de11tm.isan_meta_transactions)
""")

curs_tgt.execute("""delete from de11tm.isan_meta_transactions""")

curs_tgt.execute("""insert into de11tm.isan_meta_transactions (max_update_dt)
                    Select 
                        max(trans_dt) as max_dt
                    from de11tm.isan_stg_transactions""")







# 1.5 Работаем с таблицей terminals

# Очищаем stg слой
curs_tgt.execute ("""Delete from de11tm.isan_stg_terminals""")

# загружаем данные в stg- слой

df = pd.read_csv ('/home/de11tm/isan/project/terminals_'+current_dt+'.xlsx')

curs_tgt.executemany("""insert into de11tm.isan_stg_terminals
                       (terminal_id, terminal_type, terminal_city, terminal_address)
                       values (%s, %s,%s,%s)""",  df.values.tolist())


#insert в таблицу- приемник
curs_tgt.execute("""insert into de11tm.isan_dwh_dim_terminals_hist
                        (terminal_id,
		         terminal_type,
		         terminal_city,
		         terminal_address,
                         effective_from,
		         effective_to, 
	                 deleted_flg)
                    select 
                        terminal_id,
                        terminal_type,
                        terminal_city,
                        terminal_address,
                        current_date,
                        '5999-12-31',
                        0
                    from de11tm.isan_stg_terminals stg
                    where stg.terminal_id not in (select terminal_id from de11tm.isan_dwh_dim_terminals_hist)
""")

#обрабатываем в таблице-приемнике записи, которые были обновлены на источнике
# обработка update

curs_tgt.execute( """update de11tm.isan_dwh_dim_terminals_hist targ
                        set
                        effective_to = current_date - interval '1 second',
                        deleted_flg = 1
                      from (select 
                                stg.terminal_id,
                                current_date
                            from de11tm.isan_stg_terminals stg
                                inner join de11tm.isan_dwh_dim_terminals_hist tgt
                                on stg.terminal_id = tgt.terminal_id
                                and tgt.effective_to = cast('5999-12-31' as timestamp)
                                and tgt.deleted_flg = 0
                            where (stg.terminal_type != tgt.terminal_type)
                               or (stg.terminal_city != tgt.terminal_city)
                               or (stg.terminal_address != tgt.terminal_address)
                            ) tmp
                        where targ.terminal_id = tmp.terminal_id
                              and targ.effective_to = cast('5999-12-31' as timestamp)
                              and targ.deleted_flg = 0
                              """)

curs_tgt.execute( """insert into de11tm.isan_dwh_dim_terminals_hist
                          select  stg.terminal_id,
	                          stg.terminal_type,
		                  stg.terminal_city,
                                  stg.terminal_address,
                                  current_date,
		                  '5999-12-31', 
                                  0 as DELETED_FLG 
                          from de11tm.isan_stg_terminals stg
                                  inner join de11tm.isan_dwh_dim_terminals_hist tgt
                                  on stg.terminal_id = tgt.terminal_id
                                  and tgt.effective_to = current_date - interval '1 second'
                                  and tgt.deleted_flg = 1
                          """
)

#обрабатываем в таблице-приемнике записи, которые были удалены на источнике
#обработка delete

curs_tgt.execute( """insert into de11tm.isan_dwh_dim_terminals_hist 
                        select  tgt.terminal_id,
	                        tgt.terminal_type,
		                tgt.terminal_city,
                                tgt.terminal_address,
                                current_date,
		                '5999-12-31', 
                                1 as DELETED_FLG 
                        from de11tm.isan_dwh_dim_terminals_hist tgt
                              left join de11tm.isan_stg_terminals stg
                              on stg.terminal_id = tgt.terminal_id 
                        where tgt.effective_to = cast('5999-12-31' as timestamp)
                              and tgt.deleted_flg = 0
                              and stg.terminal_id is null""")

curs_tgt.execute( """update de11tm.isan_dwh_dim_terminals_hist targ
                        set 
                        effective_to = current_date - interval '1 second'
                      from (select tgt.terminal_id
                            from de11tm.isan_dwh_dim_terminals_hist  tgt
                              left join de11tm.isan_stg_terminals stg
                              on tgt.terminal_id = stg.terminal_id
                            where
                              tgt.effective_to = cast('5999-12-31' as timestamp)
                              and tgt.deleted_flg = 0
                              and stg.terminal_id is null) tmp
                      where targ.terminal_id = tmp.terminal_id
                            and targ.effective_to = cast('5999-12-31' as timestamp)
                            and targ.deleted_flg = 0
                        """)




# 1.6 Работаем с таблицей passport blacklist

# Очищаем stg слой
curs_tgt.execute ("""Delete from de11tm.isan_stg_passport_blacklist""")

# загружаем данные в stg- слой

df = pd.read_csv ('/home/de11tm/isan/project/passport_blacklist_'+current_dt+'.csv',delimiter = ';')

curs_tgt.executemany("""insert into de11tm.isan_stg_passport_blacklist
                       (passport_num, entry_dt)
                       values (%s, %s,%s,%s)""",  df.values.tolist())

# обработка insert	

curs_tgt.execute("""insert into de11tm.isan_dwh_fact_passport_blacklist
                                      (passport_num,
		                      effective_from
                                      )
                    select            replace (passport_num, ' ', ''),
		                      entry_dt
                    from de11tm.isan_stg_passport_blacklist
                    where entry_dt > (select max_update_dt from de11tm.isan_meta_passport_blacklist)
""")

curs_tgt.execute("""delete from de11tm.isan_meta_passport_blacklist""")

curs_tgt.execute("""insert into de11tm.isan_meta_passport_blacklist (max_update_dt)
                    Select 
                        max(entry_dt) as max_dt
                    from de11tm.isan_stg_passport_blacklist""")

    




# 2. Вычисляем мошеннические операции

# 2.1. Совершение операции при просроченном или заблокированном паспорте

# 2.1.1 номера из черного списка
curs_tgt.execute("""insert into de11tm.isan_rep_fraud (
                        event_dt,
                        passport,
                        fio,
                        phone,
                        event_type,
                        report_dt)
                    select 
                        trans.trans_date, 
                        cl.passport_num, 
                        (cl.last_name|| ' ' || cl.first_name || ' ' ||  cl.patronymic) as fio,
                        cl.phone,
                        1 as event_type,
                        now() as report_dt
                    from isan_dwh_dim_clients_hist cl
	            inner join isan_dwh_fact_passport_blacklist pas_bl
	              on pas_bl.passport_num = cl.passport_num 
                    inner join isan_dwh_dim_accounts_hist acc
                      on acc.client = cl.client_id 
                    inner join isan_dwh_dim_cards_hist card
                      on card.account_num = acc.account_num
                    inner join isan_dwh_fact_transactions trans
	              on trans.card_num = card.card_num 
	            where pas_bl.entry_dt < trans.trans_date 
""")

# 2.1.2 клиенты с просроченным паспортом
# архитектором не указано, паспорт действует включительно или нет, поэтому посчитали не включительно
curs_tgt.execute("""insert into de11tm.isan_rep_fraud (
		                event_dt,
		                passport,
		                fio,
	                        phone,
	                      	event_type,
	                       	report_dt)
                    select trans.trans_date, 
                                cl.passport_num,
                                (cl.last_name|| ' ' || cl.first_name || ' ' ||  cl.patronymic) as fio,
                                cl.phone,
                                1 as event_type,
                                now() as report_dt
                   from isan_dwh_dim_clients_hist cl
                   inner join isan_dwh_dim_accounts_hist acc
                     on acc.client = cl.client_id 
                   inner join isan_dwh_dim_cards_hist card
                     on card.account_num = acc.account_num 
                   inner join isan_dwh_fact_transactions trans
                     on trans.card_num = card.card_num 
                   where cl.passport_valid_to is not null and
                   cl.passport_valid_to < trans.trans_date 
""")

# 2.2 Cовершение операций при недействуещем договоре
# 2.2.1 ищем мощеннические операции под типом №2


curs_tgt.execute("""insert into de11tm.isan_rep_fraud (
		                event_dt,
		                passport,
		                fio,
	                        phone,
	                      	event_type,
	                       	report_dt)
                    select  
                         	trans.trans_date,
                                cl.passport_num, 
                         	(cl.last_name|| ' ' || cl.first_name || ' ' ||  cl.patronymic) as fio,
                        	cl.phone,
                        	2 as event_type,
                         	 now() as report_dt
                    from isan_dwh_dim_clients_hist cl
                    inner join isan_dwh_dim_accounts_hist acc
                      on acc.client = cl.client_id 
                    inner join isan_dwh_dim_cards_hist card
                      on card.account_num = acc.account_num 
                    inner join isan_dwh_fact_transactions trans
                      on trans.card_num = card.card_num 
                    where trans.trans_date > acc.valid_to  
""")



# 2.3 Совершение операций в разных городах в течение одного часа.


curs_tgt.execute("""insert into de11tm.isan_rep_fraud (
		                      event_dt,
		                      passport,
		                      fio,
	                        phone,
	                      	event_type,
	                       	report_dt)
                    with tmp2 as (select card_num,
                                         trans_date
                                  from (select 
                                              tmp.card_num,
		                              tmp.trans_date,
		                              tmp.terminal_city,
		                              term.terminal_city as term_city_lag 
	                                from (select 
			                              trans.card_num,
			                              trans_date,
			                              trans.trans_date - 
			                              lag (trans.trans_date) over (partition by trans.card_num order by trans.trans_date) as def_dt,
			                              term.terminal_city,
			                              lag (trans.terminal) over (partition by trans.card_num  order by trans.trans_date) as lag_term 
	                                from isan_dwh_fact_transactions trans
	                                left join de11tm.isan_dwh_dim_terminals_hist term
	                                on term.terminal_id = trans.terminal ) as tmp
                              left join de11tm.isan_dwh_dim_terminals_hist term
                              on term.terminal_id = tmp.lag_term
                              where def_dt < cast ('01:00:00' as time)) as tmp3
                              where tmp3.terminal_city != tmp3.term_city_lag)


                              select trans.trans_date,
	                                 cl.passport_num, 
	                                 (cl.last_name|| ' ' || cl.first_name || ' ' ||  cl.patronymic) as fio,
	                                 cl.phone,
	                                 3 as event_type,
	                                 now() as report_dt
                              from isan_dwh_dim_clients_hist cl
                              inner join isan_dwh_dim_accounts_hist acc
                              on acc.client = cl.client_id 
                              inner join isan_dwh_dim_cards_hist card
                              on card.account_num = acc.account_num 
                              inner join tmp2 trans
                              on trans.card_num = card.card_num 
""")

                  
# 3. Финальная часть Переименование файлов
import os

os.rename('/home/de11tm/isan/project/transactions_' + current_dt + '.csv', '/home/de11tm/isan/project/archive/transactions_' + current_dt + '.csv' + '.backup')
os.rename('/home/de11tm/isan/project/passport_blacklist_' + current_dt + '.xlsx', '/home/de11tm/isan/project/archive/passport_blacklist_' + current_dt + '.xlsx' + '.backup')
os.rename('/home/de11tm/isan/project/terminals_' + current_dt + '.xlsx', '/home/de11tm/isan/project/archive/terminals_' + current_dt + '.xlsx' + '.backup')




conn_tgt.commit()

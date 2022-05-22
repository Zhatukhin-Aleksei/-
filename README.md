# -Проект: Автоматизация ETL - процесса поиска фрода в банковских транзакциях.
---
Исходные данные -  в каталоге **/data  имется выгрузка данных из неких систем за три дня:
- Актуальный список заблокированных паспортов
(date, passport)<br>
passport_blacklist_01032021.xlsx<br>
passport_blacklist_02032021.xlsx<br>
passport_blacklist_03032021.xlsx
- Список терминалов полным срезом (terminal_id, terminal_type, terminal_city, terminal_address)<br>
terminals_01032021.xlsx<br>
terminals_02032021.xlsx <br>
terminals_03032021.xlsx
- Список транзакций за текущих день (transaction_id; transaction_date; amount; card_num; oper_type; oper_result; terminal)<br>
transactions_01032021.txt  <br>
transactions_02032021.txt <br>
transactions_03032021.txt <br>
</p>База данных банка:<br>
- Таблица аккаунтов - ACCOUNTS (ACCOUNT, VALID_TO, CLIENT, CREATE_DT, UPDATE_DT)<br>
- Таблица карт клиентов CARDS (CARD_NUM, ACCOUNT, CREATE_DT, UPDATE_DT)<br>
- Таблица клиентов CLIENTS (CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, CREATE_DT, UPDATE_DT)<br>

</p>Python скрипт читает файлы в каталоге с минимальной датой, загружает данные в хранилище даных, перемещает прочитанные файлы в каталог /backup, строит витрину отчетности из данных в хранилище по слудующим признакам:<br>
1. Совершение операций при просроченном или заблокированном паспорте.</br>
2. Совершение опрераций при недействующем договоре.</br>
3. Совершение операций в разных городах в течении одного часа</br>
4. Подбор максимальной суммы. В течении 20 минут более трех попыток снятия наличных,все неудачные, каждая последующая с меньшей суммой, последняя попытка удачная. <br>

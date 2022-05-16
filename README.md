# -Проект: Автоматизация ETL - процесса поиска фрода в банковских транзакциях.
Исходные данные -  в каталоге /data  имется выгрузка данных из неких систем за три дня:
- Актуальный список заблокированных паспортов (date, passport)<br>
passport_blacklist_01032021.xlsx<br>
passport_blacklist_02032021.xlsx<br>
passport_blacklist_03032021.xlsx
- Список терминалов полным срезом (terminal_id, terminal_type, terminal_city, terminal_address
terminals_01032021.xlsx
terminals_02032021.xlsx 
terminals_03032021.xlsx
- Список транзакций за текущих день (transaction_id; transaction_date; amount; card_num; oper_type; oper_result; terminal)
transactions_01032021.txt  
transactions_02032021.txt 
transactions_03032021.txt
Python скрипт читает файлы в каталоге с минимальной датой, загружает данные в хранилище даных, перемещает прочитанные файлы в каталог /backup, строит витрину отчетности из данных в хранилище.
Построение отчета:
1. Совершение операций при просроченном или заблокированном паспорте.
2. Совершение опрераций при недействующем договоре.
3. Совершение операций в разных городах в течении одного часа.
4. Подбор максимальной суммы. В течении 20 минут более трех попыток снятия наличных,все неудачные, каждая последующая с меньшей суммой, последняя попытка удачная. 

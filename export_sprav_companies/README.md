### add_sprav_companies

Тул для экспорта poi из справочника в народную карту. 
Принимает на вход табличку с YT с поями и экспортит все эти пои в народную карту. 

Для удобства рядом лежит скрипт, который за Вас сходит в YT и достанет poi по пермалинкам. То есть можно скормить скрипту файл со списком пермалинков и он достанет export_proto из справочника. Самому писать yql запрос не нужно.

### Опции add_sprav_companies
* **dry-run** - тестовый прогон, ничего никуда не добавляется.
* **companies-yt-path**(required) - путь к таблице на yt.
* **nmap-uid**(required) - ваш uid.
* **services-cfg-path**(required) - конфиги базы.

### Опции add_sprav_companies
* **input-file** - путь к файлу с пермалинками.
* **input-table** - путь к таблице на yt.
* **config-path** - путь к конфигам базы.
* **uid**(required) - ваш yid.
* **dry-run** - тестовый прогон, ничего никуда не добавляется.

### Про опции
* Таблица на yt - Должна содержать permalink и export_proto. Все poi из это таблицы будут добавлены в няк.
* Файл с пермалинками - Формат: список пермалинков разделенных \n. Просто в каждой строке по числу без запятых 
* Конфиги базы - конфиги unstable лежат в cfg.xml.
* uid - Можно достать из урлы в яндекс почте

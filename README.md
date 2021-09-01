# ICYB
## Описание
ICYB - бот для telegram, предсказывающий будущее расходы пользователя, основываясь на статистике за предыдущий период. 

Данные о банковских операциях бот получает из файла выписки и сохраняет их в базу данных (PostgreSQL) На данный момент реализована обработка файлов от Тинькофф банк. 

Эти данные используются для обучения ML модели, под пользователя. Она также храниться в базе, и используется для предсказания, пока бот не получит новых данных.

Для увеличения точности предсказания для пользователя можно создать список регулярных трат, дата и сумма которых известна заранее (например различные подписки). Их бот расчитывает отдельно, а затем складывает с предсказаниями модели. При обучении модели регулярные траты из общей истории исключаются.

В качестве отчета предсказания бот присылает график и таблицу движения баланса на счете.

## Информация о файлах

`bot.py`- основной файл программы. Тут описан скрипт поведения бота.

`Users.py` - описаны классы пользователя и менеджера, для работы с списком пользователей.

`DataLoader.py` -  содержет методы работы с файлами и базой данных.

`EventEngine.py` - содержет основные методы для работы с датафреймами трат.

`ML.py` - содержет методы работы с моделями и подготовки для них данных.

`Visual.py` - подготовка графиков и таблиц по данным. 

`settings.np.json` - файл с настройками для программы. Файл содержит конфиденциальную информацию, по этому вместо него в git сохранен шаблон, под именем `settings.json`
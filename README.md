Проект поисковой движок по сайту — приложение , которое позволяет индексировать страницы и осуществлять по ним быстрый поиск.
Обходит все страницы сайта начиная с главной, работает с библиотекой лемматизации слов,  
реализовано система индексации страниц сайта — систему, которая позволит подсчитывать слова на страницах сайта и по поисковому запросу определять наиболее релевантные (соответствующие поисковому запросу) страницы,
реализовано система поиска информации с использованием созданного поискового индекса;
Проект с подключенными библиотеками лемматизаторами. Содержит несколько контроллеров, сервисов и репозиторий с подключением к бд MySQL.


Принципы работы поискового движка

В конфигурационном файле перед запуском приложения задаются адреса сайтов, по которым движок должен осуществлять поиск.
Поисковый движок   самостоятельно обходит все страницы заданных сайтов и индексировать их (создавать так называемый индекс) так, чтобы потом находить наиболее релевантные страницы по любому поисковому запросу.
Пользователь присылает запрос через API движка. Запрос — это набор слов, по которым нужно найти страницы сайта.
Запрос определённым образом трансформируется в список слов, переведённых в базовую форму. Например, для существительных — именительный падеж, единственное число.
В индексе ищутся страницы, на которых встречаются все эти слова.
Результаты поиска ранжируются, сортируются и отдаются пользователю.



 
запустить проект и зайти в http://localhost:8080/ 

Описание веб-интерфейса

Веб-интерфейс (frontend-составляющая) проекта представляет собой одну веб-страницу с тремя вкладками:

Dashboard. Эта вкладка открывается по умолчанию. На ней отображается общая статистика по всем сайтам, а также детальная статистика и статус по каждому из сайтов (статистика, получаемая по запросу /api/statistics).



Management. На этой вкладке находятся инструменты управления поисковым движком — запуск и остановка полной индексации (переиндексации), а также возможность добавить (обновить) отдельную страницу по ссылке:



Search. Эта страница предназначена для тестирования поискового движка. На ней находится поле поиска, выпадающий список с выбором сайта для поиска, а при нажатии на кнопку «Найти» выводятся результаты поиска (по API-запросу /api/search):



Вся информация на вкладки подгружается путём запросов к API вашего приложения. При нажатии кнопок также отправляются запросы.

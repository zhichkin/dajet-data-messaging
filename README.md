# 1C:Enterprise 8 database C# adapter for messaging
# Microsoft SQL Server and PostgreSQL are supported

Адаптер C# для интеграции с базами данных 1С:Предприятие 8.

Проверялось на версиях 8.3.8.1433 и ниже.

Поддерживаемые СУБД: Microsoft SQL Server и PostgreSQL.

[NuGet](https://www.nuget.org/packages/DaJet.Data.Messaging) & [Telegram support channel](https://t.me/dajet_studio_group)

**Принцип работы:**
1. В конфигурации 1С создаётся нужное количество регистров сведений: входящие и исходящие очереди сообщений.
2. Структура этих регистров строго фиксирована - см. файл конфигурации 1С [**dajet-messaging.cf**](https://github.com/zhichkin/dajet-data-messaging/tree/main/1c).
4. Прикладное решение 1С работает с этими регистрами своими средствами.
5. Адаптер **DaJet.Data.Messaging** работает с этими же регистрами своими средствами.
6. Адаптер умеет самостоятельно находить эти регистры и конфигурировать необходимые объекты СУБД.
7. Адаптер не взаимодействует с кластером или сервером 1С, вся работа ведётся исключительно на уровне СУБД.

Конфигурирование объектов СУБД включает в себя создание объектов SEQUENCE (по одному на каждый регистр сведений). Это необходимо для обеспечения гарантии последовательной обработки сообщений их потребителями. Использование объектов SEQUENCE обусловлено тем, что генерация уникальных последовательных числовых значений средствами 1С затруднена, а также тем, что 1С не умеет работать с полями таблиц СУБД, имеющими признак IDENTITY.

Для исходящих очередей сообщений дополнительно создаются триггеры, которые используют созданные для них объекты SEQUENCE. Для MS SQL Server создаётся INSTEAD OF INSERT триггер. Для PostgreSQL создаётся BEFORE INSERT триггер-функция и непосредственно сам триггер, использующий её.

Примеры исходного кода для использования **DaJet.Data.Messaging** можно найти в папке проекта [**tests**](https://github.com/zhichkin/dajet-data-messaging/tree/main/src/tests).

Библиотека прошла успешные промышленные испытания, обеспечивая обмен 100-ми миллионами сообщений с одной базой данных в обе стороны ежемесячно.

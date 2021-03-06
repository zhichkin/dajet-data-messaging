# 1C:Enterprise 8 database C# adapter for messaging
## Microsoft SQL Server and PostgreSQL are supported

Адаптер C# для интеграции с базами данных 1С:Предприятие 8.

Проверялось на версиях платформы 1С:Предприятие 8.3.20.1674 и ниже.

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

Библиотека прошла успешные промышленные испытания, обеспечивая обмен 100-ми миллионов сообщений с одной базой данных в обе стороны ежемесячно. Более 20 миллионов сообщений ежедневно.

### Структура исходящей и входящей очередей сообщений (регистры сведений)

![outgoing_message_queue](https://github.com/zhichkin/dajet-data-messaging/blob/main/img/outgoing_message_queue.png) | ![incoming_message_queue](https://github.com/zhichkin/dajet-data-messaging/blob/main/img/incoming_message_queue.png)

| **Регистр сведений** | **Тип данных 1С**       | **Тип данных СУБД** | **Описание**                                          |
|----------------------|-------------------------|---------------------|-------------------------------------------------------|
| **НомерСообщения**   | Число(19,0)             | numeric(19,0)       | Порядковый номер сообщения                            |
| **Идентификатор**    | УникальныйИдентификатор | binary(16) uuid     | Идентификатор сообщения (необязательный)              |
| **Заголовки**        | Строка(0) переменная    | nvarchar(max)       | Заголовки сообщения                                   |
| **ДатаВремя**        | Дата(дата и время)      | datetime2           | Время формирования сообщения (необязательный)         |
| **ТипСообщения**     | Строка(1024) переменная | nvarchar(1024)      | Тип сообщения, например, "Справочник.Номенклатура"    |
| **ТелоСообщения**    | Строка(0) переменная    | nvarchar(max)       | Тело сообщения в формате JSON или XML                 |
| **Ссылка**           | УникальныйИдентификатор | binary(16) uuid     | Ссылка на объект 1С в теле сообщения (необязательный) |
| **Отправитель**      | Строка(36) переменная   | nvarchar(36)        | Код отправителя сообщения, передаётся в заголовках    |
| **ОписаниеОшибки**   | Строка(1024) переменная | nvarchar(1024)      | Описание ошибки, возникшей при приёме сообщения       |
| **КоличествоОшибок** | Число(2,0)              | numeric(2,0)        | Количество ошибок, возникших при приёме сообщения     |

**Примечания:**
1. В случае использования объектов СУБД sequence и/или trigger, использование поля "Идентификатор" (заполнение средствами 1С) не обязательно.
2. Все поля, имеющие пометку о необязательности, в колонке "Описание" можно не заполнять на стороне 1С.
3. Поля "ОписаниеОшибки" и "КоличествоОшибок" используются базой-приёмником 1С для блокировки входящей очереди при достижении определённого количества ошибок обработки одного и того же сообщения. Такие сообщения называются "отравленные сообщения" (poison message). Логика их обработки зависит от прикладного решения 1С.

Заголовки сообщения рекомендуется формировать в виде словаря строковых значений в формате JSON:
```JSON
{
   "Ключ1": "Значение1",
   "Ключ2": "Значение2",
   "КлючN": "ЗначениеN"
}
```
### Пример чтения сообщения из исходящей очереди
```C#
public void ConsumeOutgoingMessages()

    if (!new MetadataService()
        .UseConnectionString(MS_CONNECTION_STRING)
        .UseDatabaseProvider(DatabaseProvider.SQLServer)
        .TryOpenInfoBase(out InfoBase infoBase, out string error))
    {
        Console.WriteLine(error);
        return;
    }

    ApplicationObject queue = infoBase.GetApplicationObjectByName($"РегистрСведений.ИсходящаяОчередь");

    Console.WriteLine($"{queue.Name} [{queue.TableName}]");

    using (IMessageConsumer consumer = new MsMessageConsumer(MS_CONNECTION_STRING, in queue))
    {
        do
        {
            consumer.TxBegin(); // Начало транзакции СУБД

            // Выбираем сообщения в цикле по 100 сообщений за 1 раз
            foreach (OutgoingMessage message in consumer.Select(100))
            {
                Console.WriteLine($"Номер сообщения: {message.MessageNumber}");
		Console.WriteLine($"Идентификатор: {message.Uuid}");
		Console.WriteLine($"Дата и время: {message.DateTimeStamp}");
		Console.WriteLine($"Заголовки: {message.Headers}");
		Console.WriteLine($"Тип сообщения: {message.MessageType}");
		Console.WriteLine($"Тело сообщения: {message.MessageBody}");
		Console.WriteLine($"Ссылка: {message.Reference}");
            }

            consumer.TxCommit(); // Конец транзакции СУБД

            // Фиксация транзакции СУБД физически удаляет сообщения из очереди
            // Отмена транзакции СУБД возвращает сообщения обратно в очередь
            // Для отмены транзакции нужно вызвать исключение в блоке using до вызова consumer.TxCommit()
            // или просто выйти из блока using, не вызывая consumer.TxCommit().
            // Метод Dispose() класса MsMessageConsumer автоматически отменит незавершённую транзакцию.

            Console.WriteLine($"Получено сообщений: {consumer.RecordsAffected}");
        }
        while (consumer.RecordsAffected > 0); // Читаем очередь до конца, пока не станет пустой
    }
}
```
### Программное создание объектов СУБД SEQUENCE и TRIGGER для исходящей очереди
```C#
// Для исходящей очереди 1С это не обязательно, однако, при highload нагрузках,
// в одну и ту же миллисекунду легко может попасть несколько сообщений.
// Именно для это существует измерение "Идентификатор", но ...
// Использование этого измерения в качестве подстраховки может и будет нарушать
// требование соблюдения строгой последовательности при записи сообщений, если оно есть.
// Это обусловлено тем, что уникальные идентификаторы генерируются случайным образом.

public void ConfigureOutgoingQueue()

    if (!new MetadataService()
        .UseConnectionString(MS_CONNECTION_STRING)
        .UseDatabaseProvider(DatabaseProvider.SQLServer)
        .TryOpenInfoBase(out InfoBase infoBase, out string error))
    {
        Console.WriteLine(error);
        return;
    }

    ApplicationObject queue = infoBase.GetApplicationObjectByName($"РегистрСведений.ИсходящаяОчередь");

    Console.WriteLine($"{queue.Name} [{queue.TableName}]");

    DbInterfaceValidator validator = new DbInterfaceValidator();
    int version = validator.GetOutgoingInterfaceVersion(queue);
    
    Console.WriteLine($"Версия исходящей очереди: {version}");

    DbQueueConfigurator configurator = new DbQueueConfigurator(version, DatabaseProvider.SQLServer, MS_CONNECTION_STRING);
    // Вызов следующего метода идемпотентный - если всё уже настроено, то ничего не происходит
    configurator.ConfigureOutgoingMessageQueue(in queue, out List<string> errors);

    if (errors.Count > 0)
    {
        foreach (string error in errors)
        {
            Console.WriteLine(error);
        }
    }
    else
    {
        Console.WriteLine($"Исходящая очередь настроена успешно.");
    }
}
```
### Пример кода 1С для отправки исходящего сообщения
```JavaScript
Процедура ОтправитьИсходящееСообщение(СправочникОбъект) Экспорт
	
	Набор = РегистрыСведений.ИсходящаяОчередь.СоздатьНаборЗаписей();
	Сообщение = Набор.Добавить();
	
	Сообщение.НомерСообщения = ТекущаяУниверсальнаяДатаВМиллисекундах();
	Сообщение.Идентификатор = Новый УникальныйИдентификатор();
	Сообщение.ДатаВремя = ТекущаяДатаСеанса();
	Сообщение.Заголовки = "{ ""Sender"": ""УТ"", ""Recipients"": ""БП"" }";
	Сообщение.ТипСообщения = СправочникОбъект.Метаданные().ПолноеИмя();
	Сообщение.ТелоСообщения = СформироватьТелоСообщения(СправочникОбъект);
	Сообщение.Ссылка = СправочникОбъект.Ссылка.УникальныйИдентификатор();
	
	Набор.ОбменДанными.Загрузка = Истина;
	Набор.Записать(Ложь);	
	
КонецПроцедуры

Функция СформироватьТелоСообщения(Источник)
	
	ЗаписьJSON = Новый ЗаписьJSON();
	ЗаписьJSON.УстановитьСтроку(Новый ПараметрыЗаписиJSON(ПереносСтрокJSON.Нет, ""));
	СериализаторXDTO.ЗаписатьJSON(ЗаписьJSON, Источник, НазначениеТипаXML.Явное);
	
	Возврат ЗаписьJSON.Закрыть();
	
КонецФункции
```

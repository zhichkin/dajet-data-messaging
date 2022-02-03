DECLARE @MessageCount int = 1000;

DECLARE @messages TABLE
(
	[МоментВремени] numeric(19,0), [Идентификатор] binary(16), [ДатаВремя] datetime2,
	[Отправитель] nvarchar(36),	[Получатели] nvarchar(max), [ТипОперации] nvarchar(6),
	[ТипСообщения] nvarchar(1024), [ТелоСообщения] nvarchar(max), [ИдентификаторОбъекта] binary(16)
);

WITH cte AS
(
	SELECT TOP (@MessageCount)
		_Fld105 AS [МоментВремени],
		_Fld106 AS [Идентификатор],
		_Fld111 AS [ДатаВремя],
		_Fld107 AS [Отправитель],
		_Fld108 AS [Получатели],
		_Fld112 AS [ТипОперации],
		_Fld109 AS [ТипСообщения],
		_Fld110 AS [ТелоСообщения],
		_Fld114 AS [ИдентификаторОбъекта]
	FROM _InfoRg104 WITH (ROWLOCK, READPAST)
	ORDER BY _Fld105 ASC, _Fld106 ASC
)
DELETE cte OUTPUT
	deleted.[МоментВремени], deleted.[Идентификатор], deleted.[ДатаВремя], deleted.[Отправитель],
	deleted.[Получатели], deleted.[ТипОперации], deleted.[ТипСообщения], deleted.[ТелоСообщения],
	deleted.[ИдентификаторОбъекта]
INTO @messages;

SELECT
	[МоментВремени],
	[Идентификатор],
	[ТипОперации],
	[ТелоСообщения]
FROM
	(SELECT
		[МоментВремени],
		[Идентификатор],
		[ТипОперации],
		[ТелоСообщения],
		[ИдентификаторОбъекта],
		MAX([МоментВремени]) OVER(PARTITION BY [ИдентификаторОбъекта]) AS [Версия]
	FROM @messages) AS T
WHERE
	[ТелоСообщения] <> '' OR [МоментВремени] = [Версия]
ORDER BY
	[МоментВремени] ASC, [Идентификатор] ASC;
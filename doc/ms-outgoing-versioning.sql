DECLARE @MessageCount int = 1000;

DECLARE @messages TABLE
(
	[�������������] numeric(19,0), [�������������] binary(16), [���������] datetime2,
	[�����������] nvarchar(36),	[����������] nvarchar(max), [�����������] nvarchar(6),
	[������������] nvarchar(1024), [�������������] nvarchar(max), [��������������������] binary(16)
);

WITH cte AS
(
	SELECT TOP (@MessageCount)
		_Fld105 AS [�������������],
		_Fld106 AS [�������������],
		_Fld111 AS [���������],
		_Fld107 AS [�����������],
		_Fld108 AS [����������],
		_Fld112 AS [�����������],
		_Fld109 AS [������������],
		_Fld110 AS [�������������],
		_Fld114 AS [��������������������]
	FROM _InfoRg104 WITH (ROWLOCK, READPAST)
	ORDER BY _Fld105 ASC, _Fld106 ASC
)
DELETE cte OUTPUT
	deleted.[�������������], deleted.[�������������], deleted.[���������], deleted.[�����������],
	deleted.[����������], deleted.[�����������], deleted.[������������], deleted.[�������������],
	deleted.[��������������������]
INTO @messages;

SELECT
	[�������������],
	[�������������],
	[�����������],
	[�������������]
FROM
	(SELECT
		[�������������],
		[�������������],
		[�����������],
		[�������������],
		[��������������������],
		MAX([�������������]) OVER(PARTITION BY [��������������������]) AS [������]
	FROM @messages) AS T
WHERE
	[�������������] <> '' OR [�������������] = [������]
ORDER BY
	[�������������] ASC, [�������������] ASC;
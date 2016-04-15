# CLRProcedureImportarUtf8
drop procedure InsertWithUtf8
GO

drop assembly externa
GO

CREATE ASSEMBLY externa from 'C:\Users\fabricio.antunes\Documents\Visual Studio 2015\Projects\ProcedureInsertWithUTFConvert\ProcedureInsertWithUTFConvert\bin\Debug\ProcedureInsertWithUTFConvert.dll' WITH PERMISSION_SET = SAFE
GO
CREATE PROCEDURE InsertWithUtf8 @table nvarchar(150) ,@name nvarchar(150) 
AS
EXTERNAL NAME externa.StoredProcedures.ProcedureInsertWithUTFConvert

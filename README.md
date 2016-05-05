# CLRProcedureImportarUtf8
## Create the procedure on sql server management studio

IF Exists ( Select * from sys.procedures where name = 'Your_Procedure_Name')
drop procedure Your_Procedure_Name
GO

IF EXISTS (select * from sys.assemblies where name = 'Your_Assembly_Name')
drop assembly Your_Assembly_Name
GO

ALTER DATABASE [Your_Database] SET TRUSTWORTHY ON; 
GO
CREATE ASSEMBLY Your_Assembly_Name from 'C:\local\to\your\Procedure.dll' WITH PERMISSION_SET = EXTERNAL_ACCESS
GO

CREATE PROCEDURE Your_Procedure_Name @de nvarchar(150) ,@para nvarchar(150),@schemade nvarchar(150) ,@schemapara nvarchar(150) , @count int
AS
EXTERNAL NAME Your_Assembly_Name.StoredProcedures.ProcedureInsertWithUTFConvert

﻿using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Text;
using System.Collections.Generic;

public partial class StoredProcedures
{

    public static string ConverteText(SqlString text)
    {
        if (text.IsNull)
            return null;
        byte[] bytes = Encoding.Default.GetBytes(text.ToString());
        return Encoding.UTF8.GetString(bytes);
    }
    public class ListaColunas
    {
        public List<Object> values = new List<Object>();
    }
    private static StringBuilder GetCreatePrimaryScript(StringBuilder primary_builder, string primary, string tname)
    {
        if (!String.IsNullOrWhiteSpace(primary))
            if (String.IsNullOrWhiteSpace(primary_builder.ToString()))
            {

                primary_builder.AppendFormat("CONSTRAINT \"PK_{0}\" PRIMARY KEY CLUSTERED ([{1}", tname, primary);
            }
            else
            {
                primary_builder.AppendFormat("], [{0}", primary);
            }
        return primary_builder;
    }
    private static string GetCreateTableScript(DataTable dt, string schema, string tableName, SqlConnection conn)
    {
        string snip = string.Empty;
        string primary = string.Empty;
        StringBuilder sql = new StringBuilder();
        StringBuilder primary_builder = new StringBuilder();

        sql.AppendFormat("IF OBJECT_ID('" + schema + tableName + "', 'U') IS NOT NULL\r\n DROP TABLE " + schema + tableName + "; \r\n");
        sql.AppendFormat("CREATE TABLE {0}{1}\r\n(\r\n", schema, tableName);
        for (int i = 0; i < dt.Rows.Count; i++)
        {
            DataRow dr = dt.Rows[i];
            primary = GetColumnPrimarySql(dr, tableName, conn);
            primary_builder = GetCreatePrimaryScript(primary_builder, primary, tableName.ToUpper());
            snip = GetColumnSql(dr, tableName, conn);
            sql.AppendFormat((i < dt.Rows.Count - 1) ? snip : snip.TrimEnd(',', '\r', '\n'));
        }
        if (!String.IsNullOrWhiteSpace(primary_builder.ToString()))
        {
            primary_builder.AppendFormat("])");
        }
        sql.AppendFormat("\r\n\r\n");
        sql.AppendFormat(primary_builder.ToString());
        sql.AppendFormat("\r\n)");

        return sql.ToString();
    }

    private static string changeValue(string dataType, bool hasSize, string ColumnSize, bool hasPrecisionAndScale, string NumericPrecision, string NumericScale)
    {
        int p_ColumnSize;
        int.TryParse(ColumnSize, out p_ColumnSize);
        string converted = "";
        switch (dataType)
        {
            case "char":
            case "varchar":
                if (hasSize) { if (p_ColumnSize <= 8000) { converted = "VARCHAR (" + ColumnSize + ")"; } else { converted = "VARCHAR (MAX)"; } }
                break;
            case "text":
            case "long varchar":
                converted = "VARCHAR (MAX)";
                break;
            case "long binary":
            case "image":
                converted = "VARBINARY (MAX)";
                break;
            case "datetime":
                converted = "datetime2";
                break;
            case "double":
                converted = "real";
                break;
            case "float":
                converted = "real";
                converted = (hasSize) ? "(" + ColumnSize + ")" : (hasPrecisionAndScale) ? "(" + NumericPrecision + ")" : "";
                converted = dataType + " " + converted;
                break;
            default:
                converted = (hasSize) ? "(" + ColumnSize + ")" : (hasPrecisionAndScale) ? "(" + NumericPrecision + "," + NumericScale + ")" : "";
                converted = dataType + " " + converted;
                break;
        }
        return converted;
    }

    private static string GetColumnPrimarySql(DataRow dr, string tname, SqlConnection conn)
    {
        string tablecol = dr["BaseColumnName"].ToString();
        bool inPrimaryKey = false;
        using (SqlCommand cmds = new SqlCommand(" SELECT * FROM  OPENQUERY([Sybase],'Select * from  [Sybase].IcetranProducao.sys.SYSCOLUMNS where tname = ''" + tname + "'' and cname = ''" + tablecol + "''') ", conn))
        {
            SqlTransaction transactionset;
            transactionset = conn.BeginTransaction("SetTabela");
            cmds.Transaction = transactionset;
            using (SqlDataReader reader = cmds.ExecuteReader())
            {
                while (reader.Read())
                {
                    inPrimaryKey = reader["in_primary_key"].ToString() == "Y";
                }
                reader.Close();
            }
            transactionset.Commit();
        }
        return (inPrimaryKey) ? tablecol : "";
    }

    private static string GetColumnSql(DataRow dr, string tablename, SqlConnection conn)
    {
        StringBuilder sql = new StringBuilder();

        string ColumnSize = "";
        string DataTypeName = "";
        string NumericPrecision = "";
        string NumericScale = "";
        string ColumnName = "";
        bool hasSize = false;
        bool hasPrecisionAndScale = false;

        ColumnName = dr["ColumnName"].ToString();
        DataTypeName = dr["DataTypeName"].ToString();
        hasSize = HasSize(dr["DataTypeName"].ToString());
        hasPrecisionAndScale = HasPrecisionAndScale(dr["DataTypeName"].ToString());

        string tablecol = dr["BaseColumnName"].ToString();

        if (hasSize)
            ColumnSize = dr["ColumnSize"].ToString();

        if (hasPrecisionAndScale)
        {
            NumericPrecision = dr["NumericPrecision"].ToString();
            NumericScale = dr["NumericScale"].ToString();
        }

        bool IsUnique = (dr["IsUnique"].ToString().ToUpper() == "TRUE");
        bool IsKey = (dr["IsKey"].ToString().ToUpper() == "TRUE");
        bool AllowDBNull = (dr["AllowDBNull"].ToString().ToUpper() == "TRUE");
        bool IsAliased = (dr["IsAliased"].ToString().ToUpper() == "TRUE");
        bool IsExpression = (dr["IsExpression"].ToString().ToUpper() == "TRUE");
        bool IsIdentity = (dr["IsIdentity"].ToString().ToUpper() == "TRUE");
        bool IsAutoIncrement = (dr["IsAutoIncrement"].ToString().ToUpper() == "TRUE");
        bool IsRowVersion = (dr["IsRowVersion"].ToString().ToUpper() == "TRUE");
        bool IsHidden = (dr["IsHidden"].ToString().ToUpper() == "TRUE");
        bool IsLong = (dr["IsLong"].ToString().ToUpper() == "TRUE");
        bool IsReadOnly = (dr["IsReadOnly"].ToString().ToUpper() == "TRUE");
        bool IsColumnSet = (dr["IsColumnSet"].ToString().ToUpper() == "TRUE");
        string auxAutoIncrement = "";
        bool inPrimaryKey = false;
        using (SqlCommand cmds = new SqlCommand(" SELECT * FROM  OPENQUERY([Sybase],'Select * from  [Sybase].IcetranProducao.sys.SYSCOLUMNS where tname = ''" + tablename + "'' and cname = ''" + tablecol + "''') ", conn))
        {
            SqlTransaction transactionset;
            transactionset = conn.BeginTransaction("SetTabela");
            cmds.Transaction = transactionset;
            using (SqlDataReader reader = cmds.ExecuteReader())
            {
                while (reader.Read())
                {
                    auxAutoIncrement = reader["default_value"].ToString();
                    inPrimaryKey = reader["in_primary_key"].ToString() == "Y";
                }
                reader.Close();
            }
            transactionset.Commit();
        }
        switch (auxAutoIncrement)
        {
            case "current timestamp":
                auxAutoIncrement = "GETDATE()";
                break;
            default:
                break;
        }
        sql.AppendFormat("\t[{0}] {1} {2} {3} {4},\r\n",
               ColumnName,
               changeValue(DataTypeName, hasSize, ColumnSize, hasPrecisionAndScale, NumericPrecision, NumericScale),
               (IsIdentity) ? "IDENTITY" : "",
               (IsAutoIncrement) ? "IDENTITY" : String.IsNullOrEmpty(auxAutoIncrement) ? "" : (auxAutoIncrement.ToUpper() == "AUTOINCREMENT") ? "IDENTITY" : " DEFAULT  " + auxAutoIncrement,
               (AllowDBNull) ? (String.IsNullOrEmpty(auxAutoIncrement)? " NULL" : (auxAutoIncrement.ToUpper() == "AUTOINCREMENT")?  " NOT NULL": " NULL ") : "  NOT NULL"
               );
        return sql.ToString();
    }

    private static string GetPrimaryForOrder(DataTable dt, string schema, string tableName, SqlConnection conn)
    {
        string primary = string.Empty;
        for (int i = 0; i < dt.Rows.Count; i++)
        {
            DataRow dr = dt.Rows[i];
            if (String.IsNullOrEmpty(primary))
                primary = GetColumnPrimarySql(dr, tableName, conn);
        }
        return primary;
    }

    private static int GetPrimaryForOrderOrdinal(DataTable dt, string schema, string tableName, SqlConnection conn)
    {
        int primary = 0;
        for (int i = 0; i < dt.Rows.Count; i++)
        {
            DataRow dr = dt.Rows[i];
            if (primary == 0)
                primary = GetColumnPrimarySqlOrdinal(dr, tableName, conn);
        }
        return primary;
    }
    private static int GetColumnPrimarySqlOrdinal(DataRow dr, string tname, SqlConnection conn)
    {
        string tablecol = dr["BaseColumnName"].ToString();
        int colno = 0;
        bool inPrimaryKey = false;
        using (SqlCommand cmds = new SqlCommand(" SELECT * FROM  OPENQUERY([Sybase],'Select * from  [Sybase].IcetranProducao.sys.SYSCOLUMNS where tname = ''" + tname + "'' and cname = ''" + tablecol + "''') ", conn))
        {
            SqlTransaction transactionset;
            transactionset = conn.BeginTransaction("SetTabela");
            cmds.Transaction = transactionset;
            using (SqlDataReader reader = cmds.ExecuteReader())
            {
                while (reader.Read())
                {
                    inPrimaryKey = reader["in_primary_key"].ToString() == "Y";
                    if (inPrimaryKey)
                        colno = Convert.ToInt32(reader["colno"].ToString()) - 1;
                }
                reader.Close();
            }
            transactionset.Commit();
        }
        return (inPrimaryKey) ? colno : 0;
    }

    [SqlProcedure()]
    public static void ProcedureInsertWithUTFConvert(
        SqlString de, SqlString para, SqlString schemade, SqlString schemapara, SqlInt32 count)
    {
        bool hasIdenity = false;
        string primaryfororder = "";
        int primaryforordinal = 0;
        List<string> columns = new List<string>();
        List<SqlDbType> tipos = new List<SqlDbType>();
        using (SqlConnection conn = new SqlConnection("context connection=true"))
        {
            int perpage = 1000;
            int totalpages = Convert.ToInt32(Math.Ceiling((decimal)((count.ToSqlDecimal() * (decimal)1.0) / perpage)));

            conn.Open();
            SqlCommand cmdset = conn.CreateCommand();
            SqlTransaction transactionset;
            transactionset = conn.BeginTransaction("SetTabela");
            cmdset.CommandText = "SET FMTONLY ON;SET NOCOUNT ON; select * from OPENQUERY([SYBASE],' Select * from " + schemade.ToString() + de.ToString() + "'); SET FMTONLY OFF;SET NOCOUNT OFF;";
            cmdset.Transaction = transactionset;

            SqlDataReader reader = cmdset.ExecuteReader(CommandBehavior.KeyInfo);
            DataTable schemaTable = reader.GetSchemaTable();

            foreach (DataRow row in schemaTable.Rows)
            {
                //foreach (DataColumn myProperty in schemaTable.Columns){SqlContext.Pipe.Send(myProperty.ColumnName + " = " + row[myProperty].ToString());}
                columns.Add(row["ColumnName"].ToString());
                SqlDbType type = (SqlDbType)(int)row["ProviderType"];
                tipos.Add(type);
            }
            reader.Close();
            transactionset.Commit();

            SqlContext.Pipe.Send("Recriando Tabela " + schemapara.ToString() + para.ToString());
            string script = GetCreateTableScript(schemaTable, schemapara.ToString(), para.ToString(), conn);  //Gera Script de Criação da tabela 
            //SqlContext.Pipe.Send(script); // Descomentar linha para exibir o script de criação da tabela
            SqlCommand cmd = conn.CreateCommand();
            SqlTransaction transactioncria; // transaction de criação da tabela. 
            transactioncria = conn.BeginTransaction("CriaTabela");
            cmd.CommandText = script;
            cmd.Transaction = transactioncria;

            SqlDataReader readerRecreate = cmd.ExecuteReader();
            readerRecreate.Close();
            transactioncria.Commit(); // encerra transaciton de criação da tabela


            SqlContext.Pipe.Send("Verificando necessidade de ativar o Identity_Insert ");
            SqlTransaction transactioncheckidendity; // transaction do Identity_Insert. 
            transactioncheckidendity = conn.BeginTransaction("hasIdentity");
            SqlCommand cmdHasIdentity = conn.CreateCommand();
            cmdHasIdentity.CommandText = "SELECT OBJECTPROPERTY(OBJECT_ID('" + schemapara.ToString() + para.ToString() + "'), 'TableHasIdentity') ";
            cmdHasIdentity.Transaction = transactioncheckidendity;

            using (SqlDataReader readerHasIdentity = cmdHasIdentity.ExecuteReader())
            {
                while (readerHasIdentity.Read())
                {
                    hasIdenity = readerHasIdentity[0].ToString() == "1";
                }
                readerHasIdentity.Close();
            }
            transactioncheckidendity.Commit();

            if (hasIdenity)
            {

                //Ativa a opção Identity_Insert
                SqlTransaction transactionsetidentityon; // transaction do Identity_Insert. 
                transactionsetidentityon = conn.BeginTransaction("IdentityInsert");
                SqlCommand cmdSetIdentity = conn.CreateCommand();
                cmdSetIdentity.CommandText = "SET IDENTITY_INSERT " + schemapara.ToString() + para.ToString() + " ON";
                cmdSetIdentity.Transaction = transactionsetidentityon;

                SqlDataReader readerSetIdentity = cmdSetIdentity.ExecuteReader();
                readerSetIdentity.Close();
                transactionsetidentityon.Commit();

                primaryfororder = GetPrimaryForOrder(schemaTable, schemapara.ToString(), para.ToString(), conn);
                primaryforordinal = GetPrimaryForOrderOrdinal(schemaTable, schemapara.ToString(), para.ToString(), conn);

            }
            SqlContext.Pipe.Send("======== Adicionando campos a tabela ============");

            int id = 0;
            for (int page = 0; page < totalpages; page++)
            {
                try
                {
                    SqlTransaction transaction;
                    // Start a local transaction.
                    transaction = conn.BeginTransaction("SampleTransaction");

                    int startat = (page * perpage) + 1;

                    string querypaginada;

                    if (hasIdenity && (!String.IsNullOrEmpty(primaryfororder)))
                    {
                        querypaginada = "select * FROM OPENQUERY([SYBASE],'Select TOP " + perpage + " *  FROM " + schemade.ToString() + de.ToString() + " Where " + primaryfororder + " > " + id + " Order by " + (primaryforordinal + 1) + "')";
                    }
                    else
                    {
                        querypaginada = "select * FROM OPENQUERY([SYBASE],'Select TOP " + perpage + "  START AT " + startat + " *  FROM " + schemade.ToString() + de.ToString() + " Order by 1')";
                    }
                    List<ListaColunas> values = new List<ListaColunas>();
                    using (SqlCommand selectFields = new SqlCommand(querypaginada
                     ,
                     conn))
                    {
                        SqlContext.Pipe.Send("Iniciando na linha :" + startat);
                        SqlContext.Pipe.Send("======= Hora Inicio: " + DateTime.Now);
                        selectFields.Transaction = transaction;

                        DataSet selectResults = new DataSet();
                        using (SqlDataReader fieldsReader = selectFields.ExecuteReader())
                        {
                            while (fieldsReader.Read())
                            {
                                var novosCampos = new ListaColunas();
                                List<Object> campos = new List<Object>();
                                for (int i = 0; i < fieldsReader.FieldCount; i++)
                                {
                                    campos.Add(fieldsReader[i]);
                                }
                                novosCampos.values = campos;
                                values.Add(novosCampos);
                                if (hasIdenity)
                                {
                                    id = Convert.ToInt32(fieldsReader[primaryforordinal].ToString());
                                }
                            }
                            fieldsReader.Close();
                        }
                        transaction.Commit();
                        SqlContext.Pipe.Send("======= Hora Término: " + DateTime.Now);
                    }
                    //loop de 'Insert' de valores
                    InsertValues(para, schemapara, conn, values, columns, tipos);
                }
                catch (Exception e)
                {
                    SqlContext.Pipe.Send("=================== FALHA : " + e.ToString() + "======================");
                }
            }
            if (hasIdenity)
            {
                SqlTransaction transactionsetidentityoff; // transaction do Identity_Insert. 
                transactionsetidentityoff = conn.BeginTransaction("IdentityInsert");
                SqlCommand cmdSetIdentityoff = conn.CreateCommand();
                cmdSetIdentityoff.CommandText = "SET IDENTITY_INSERT " + schemapara.ToString() + para.ToString() + " OFF";
                cmdSetIdentityoff.Transaction = transactionsetidentityoff;

                SqlDataReader readerSetIdentityoff = cmdSetIdentityoff.ExecuteReader();
                readerSetIdentityoff.Close();
                transactionsetidentityoff.Commit();
            }
            conn.Close();
        }
    }

    public static void InsertValues(SqlString para, SqlString schemapara, SqlConnection conn, List<ListaColunas> values, List<string> columns, List<SqlDbType> tipos)
    {
        foreach (ListaColunas value in values)
        {
            string commandoInsert = "INSERT " + schemapara.ToString() + para.ToString() + "(";
            foreach (var col in columns)
                commandoInsert += col + ",";

            commandoInsert = commandoInsert.TrimEnd(',');

            commandoInsert += " ) VALUES ( ";
            foreach (var col in columns)
                commandoInsert += " @" + col.ToLower().Trim() + " ,";

            commandoInsert = commandoInsert.TrimEnd(',');
            commandoInsert += ")";
            var cnt = 0;

            using (SqlCommand objInsertCommand = new SqlCommand(commandoInsert, conn))
            {
                SqlTransaction transaction;

                // Start a local transaction.
                transaction = conn.BeginTransaction("InsertTransaction");

                objInsertCommand.Transaction = transaction;
                foreach (var val in value.values)
                {

                    objInsertCommand.Parameters.Add("@" + columns[cnt].ToLower().Trim(), tipos[cnt]);

                    if (tipos[cnt] == SqlDbType.VarChar || tipos[cnt] == SqlDbType.NVarChar || tipos[cnt] == SqlDbType.Text || tipos[cnt] == SqlDbType.NText)
                    { objInsertCommand.Parameters["@" + columns[cnt].ToLower().Trim()].Value = ConverteText(val as String); }
                    else { objInsertCommand.Parameters["@" + columns[cnt].ToLower().Trim()].Value = val; }

                    cnt = cnt + 1;
                }
                objInsertCommand.ExecuteNonQuery();
                transaction.Commit();
            }
        }

    }


    private static bool HasSize(string dataType)
    {
        Dictionary<string, bool> dataTypes = new Dictionary<string, bool>();
        dataTypes.Add("bigint", false);
        dataTypes.Add("binary", true);
        dataTypes.Add("bit", false);
        dataTypes.Add("char", true);
        dataTypes.Add("date", false);
        dataTypes.Add("datetime", false);
        dataTypes.Add("datetime2", false);
        dataTypes.Add("datetimeoffset", false);
        dataTypes.Add("decimal", false);
        dataTypes.Add("float", false);
        dataTypes.Add("geography", false);
        dataTypes.Add("geometry", false);
        dataTypes.Add("hierarchyid", false);
        dataTypes.Add("image", true);
        dataTypes.Add("int", false);
        dataTypes.Add("money", false);
        dataTypes.Add("nchar", true);
        dataTypes.Add("ntext", true);
        dataTypes.Add("numeric", false);
        dataTypes.Add("nvarchar", true);
        dataTypes.Add("real", false);
        dataTypes.Add("smalldatetime", false);
        dataTypes.Add("smallint", false);
        dataTypes.Add("smallmoney", false);
        dataTypes.Add("sql_variant", false);
        dataTypes.Add("sysname", false);
        dataTypes.Add("text", true);
        dataTypes.Add("time", false);
        dataTypes.Add("timestamp", false);
        dataTypes.Add("tinyint", false);
        dataTypes.Add("uniqueidentifier", false);
        dataTypes.Add("varbinary", true);
        dataTypes.Add("varchar", true);
        dataTypes.Add("xml", false);
        if (dataTypes.ContainsKey(dataType))
            return dataTypes[dataType];
        return false;
    }

    private static bool HasPrecisionAndScale(string dataType)
    {
        Dictionary<string, bool> dataTypes = new Dictionary<string, bool>();
        dataTypes.Add("bigint", false);
        dataTypes.Add("binary", false);
        dataTypes.Add("bit", false);
        dataTypes.Add("char", false);
        dataTypes.Add("date", false);
        dataTypes.Add("datetime", false);
        dataTypes.Add("datetime2", false);
        dataTypes.Add("datetimeoffset", false);
        dataTypes.Add("decimal", true);
        dataTypes.Add("float", true);
        dataTypes.Add("geography", false);
        dataTypes.Add("geometry", false);
        dataTypes.Add("hierarchyid", false);
        dataTypes.Add("image", false);
        dataTypes.Add("int", false);
        dataTypes.Add("money", false);
        dataTypes.Add("nchar", false);
        dataTypes.Add("ntext", false);
        dataTypes.Add("numeric", false);
        dataTypes.Add("nvarchar", false);
        dataTypes.Add("real", true);
        dataTypes.Add("smalldatetime", false);
        dataTypes.Add("smallint", false);
        dataTypes.Add("smallmoney", false);
        dataTypes.Add("sql_variant", false);
        dataTypes.Add("sysname", false);
        dataTypes.Add("text", false);
        dataTypes.Add("time", false);
        dataTypes.Add("timestamp", false);
        dataTypes.Add("tinyint", false);
        dataTypes.Add("uniqueidentifier", false);
        dataTypes.Add("varbinary", false);
        dataTypes.Add("varchar", false);
        dataTypes.Add("xml", false);
        if (dataTypes.ContainsKey(dataType))
            return dataTypes[dataType];
        return false;
    }
}
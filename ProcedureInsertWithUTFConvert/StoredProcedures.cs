using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Text;

public partial class StoredProcedures
{
    public static string ConverteText(SqlString text)
    {
        if (text.IsNull)
            return null;

        var myString = text.ToString();
        byte[] bytes = Encoding.Default.GetBytes(myString);
        myString = Encoding.UTF8.GetString(bytes);

        return myString;
    }

    [SqlProcedure()]
    public static void ProcedureInsertWithUTFConvert(
        SqlString table, SqlString name)
    {
       
        using (SqlConnection conn = new SqlConnection("context connection=true"))
        {
            conn.Open();
            using (SqlCommand selectFields = new SqlCommand(
             "SELECT " +
             " * " +
             " FROM @tabela"
             ,
             conn))
            {
                SqlParameter tabelaParam = selectFields.Parameters.Add(
                    "@tabela",
                    SqlDbType.VarChar);
                tabelaParam.Value = table;

                using (SqlDataReader fieldsReader = selectFields.ExecuteReader())
                {
                    while (fieldsReader.Read())
                    {
                        SqlString emailAddress = fieldsReader.GetSqlString(1);
                      

                    }
                }
            }

        }

    }
}
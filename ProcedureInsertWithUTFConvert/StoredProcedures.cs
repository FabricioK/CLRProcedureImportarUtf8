using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Text;
using System.IO;
using System.Collections.Generic;

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
    public class ListaColunas {
         public List<string> values = new List<string>();
    }
    [SqlProcedure()]
    public static void ProcedureInsertWithUTFConvert(
        SqlString de, SqlString para)
    {
        List<ListaColunas> values = new List<ListaColunas>();
        List<string> columns = new List<string>();
        List<Type> tipos = new List<Type>();
        using (SqlConnection conn = new SqlConnection("context connection=true"))
        {
            conn.Open();
            using (SqlCommand selectFields = new SqlCommand(
             "SELECT  * FROM " + de.ToString()
             ,
             conn))
            {
                DataSet selectResults = new DataSet();
                using (SqlDataReader fieldsReader = selectFields.ExecuteReader())
                {                  

                    for (int i = 0; i < fieldsReader.FieldCount; i++)
                    {
                        columns.Add(fieldsReader.GetName(i));
                    }

                    for (int i = 0; i < fieldsReader.FieldCount; i++)
                    {
                        tipos.Add(fieldsReader.GetFieldType(i));
                    }

                    while (fieldsReader.Read())
                    {
                        fieldsReader.GetFieldType(0);
                        var novosCampos =  new ListaColunas ();
                        List<string> campos = new List<string>();
                        for (int i = 0; i < fieldsReader.FieldCount; i++)
                        {
                            campos.Add(fieldsReader[i].ToString());
                        }
                        novosCampos.values = campos;
                    }
                }
            }

            foreach (ListaColunas value in values)
            {
                string  commandoInsert = "INSERT "+ para.ToString()+ "(";
                foreach (var col in columns)
                    commandoInsert = commandoInsert + col + ",";

                commandoInsert = commandoInsert.TrimEnd(',');

                commandoInsert  = commandoInsert + ") VALUES (";
                var cnt = 0;
                foreach (var val in value.values)
                {
                    tipos[cnt].
                    commandoInsert = commandoInsert + ConverteText(val) + ",";

                    cnt =+1;
                }
                commandoInsert = commandoInsert + ")";

                using (SqlCommand objInsertCommand = new SqlCommand(commandoInsert, conn))
                {
                    objInsertCommand.ExecuteNonQuery();
                }
            }
            conn.Close();
        }
    }
}
using System;
using System.Data;
using System.Data.Sql;
using Microsoft.SqlServer.Server;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Xml;
using System.Text.RegularExpressions;
using System.IO;
using System.Linq;
using System.Net;
//using System.Net.Http;
using System.Text;
using System.Runtime.Serialization.Json;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Web;


public class CLRTriggers
{

    [SqlTrigger(Name = @"clr_Trg_RatePlanInventory_AftInsUpdDel_toKafka", Target = "[dbo].[RatePlanInventory]", Event = "FOR INSERT, UPDATE, DELETE")]
    public static void clr_Trg_RatePlanInventory_AftInsUpdDel_toKafka()
    {
        //Kafka RestApi Properties
        //CLR Trigger Properties
        SqlCommand command;
        SqlTriggerContext triggContext = SqlContext.TriggerContext;
        SqlPipe pipe = SqlContext.Pipe;
        SqlDataReader reader;
        switch (triggContext.TriggerAction)
        {
            case TriggerAction.Insert:
                // Retrieve the connection that the trigger is using
                using (SqlConnection connection
                   = new SqlConnection(@"context connection=true"))
                {
                    connection.Open();
                    command = new SqlCommand(@"SELECT * FROM INSERTED;",
                       connection);
                    reader = command.ExecuteReader();
                    reader.Read();

                    RatePlanInventory ratePlanInv = new RatePlanInventory
                    {

                        SKUGroupID = (int)reader[0],
                        RatePlanID = (int)reader[1],
                        StayDate = reader[2].ToString(),
                        LogSeqNbr = (int)reader[3],
                        SupplierLogSeqNbr = (int)reader[4],
                        InventoryCntAvailable = (int)reader[5],
                        InventoryCntSold = (int)reader[6],
                        ChangeRequestID = reader[7].ToString(),
                        UpdateDate = reader[8].ToString(),
                        UpdateTPID = (int)reader[9],
                        UpdateTUID = (int)reader[10],
                        ChangeType = "insert"
                    };

                    string json = JsonConvert.SerializeObject(ratePlanInv, Newtonsoft.Json.Formatting.None);
                    
                    reader.Close();
                    
                    pipe.Send("You inserted: " + json);
                    try
                    {
                        KafkaSendMessageAsync("RatePlanInventory", json, ratePlanInv.SKUGroupID.ToString(), pipe);
                    }
                    catch (System.Net.WebException we)
                    {
                        pipe.Send(we.Message);
                    }


                }

                break;

            case TriggerAction.Update:
                // Retrieve the connection that the trigger is using
                using (SqlConnection connection
                   = new SqlConnection(@"context connection=true"))
                {
                    connection.Open();
                    command = new SqlCommand(@"SELECT * FROM INSERTED;",
                       connection);
                    reader = command.ExecuteReader();
                    reader.Read();

                    RatePlanInventory ratePlanInv = new RatePlanInventory
                    {

                        SKUGroupID = (int)reader[0],
                        RatePlanID = (int)reader[1],
                        StayDate = reader[2].ToString(),
                        LogSeqNbr = (int)reader[3],
                        SupplierLogSeqNbr = (int)reader[4],
                        InventoryCntAvailable = (int)reader[5],
                        InventoryCntSold = (int)reader[6],
                        ChangeRequestID = reader[7].ToString(),
                        UpdateDate = reader[8].ToString(),
                        UpdateTPID = (int)reader[9],
                        UpdateTUID = (int)reader[10],
                        ChangeType = "updated"
                    };

                    string json = JsonConvert.SerializeObject(ratePlanInv, Newtonsoft.Json.Formatting.Indented);
                    pipe.Send(@"You updated: '" + json);

                    for (int columnNumber = 0; columnNumber < triggContext.ColumnCount; columnNumber++)
                    {
                        pipe.Send("Updated column "
                           + reader.GetName(columnNumber) + "? "
                           + triggContext.IsUpdatedColumn(columnNumber).ToString());
                    }

                    reader.Close();
                    try
                    {
                        KafkaSendMessageAsync("RatePlanInventory", json, ratePlanInv.SKUGroupID.ToString(), pipe);
                    }
                    catch (System.Net.WebException we)
                    {
                        pipe.Send(we.Message);
                    }

                }

                break;

            case TriggerAction.Delete:
                using (SqlConnection connection
                   = new SqlConnection(@"context connection=true"))
                {
                    connection.Open();
                    command = new SqlCommand(@"SELECT * FROM DELETED;",
                       connection);
                    reader = command.ExecuteReader();

                    if (reader.HasRows)
                    {
                        pipe.Send(@"You deleted the following rows:");
                        while (reader.Read())
                        {
                            RatePlanInventory ratePlanInv = new RatePlanInventory
                            {

                                SKUGroupID = (int)reader[0],
                                RatePlanID = (int)reader[1],
                                StayDate = reader[2].ToString(),
                                LogSeqNbr = (int)reader[3],
                                SupplierLogSeqNbr = (int)reader[4],
                                InventoryCntAvailable = (int)reader[5],
                                InventoryCntSold = (int)reader[6],
                                ChangeRequestID = reader[7].ToString(),
                                UpdateDate = reader[8].ToString(),
                                UpdateTPID = (int)reader[9],
                                UpdateTUID = (int)reader[10],
                                ChangeType = "deleted"
                            };
                            string json = JsonConvert.SerializeObject(ratePlanInv, Newtonsoft.Json.Formatting.Indented);

                            pipe.Send("You deleted: " + json);
                            try
                            {
                                KafkaSendMessageAsync("RatePlanInventory", json, ratePlanInv.SKUGroupID.ToString(), pipe);
                            }
                            catch (System.Net.WebException we)
                            {
                                pipe.Send(we.Message);
                            }
                        }

                        reader.Close();


                        //alternately, to just send a tabular resultset back:
                        //pipe.ExecuteAndSend(command);
                    }
                    else
                    {
                        pipe.Send("No rows affected.");
                    }
                }

                break;
        }
    }

    public static void KafkaSendMessageAsync(string topic, string json, string key, SqlPipe pipe)
    {
        json = HttpUtility.UrlEncode(json);

        Uri uri = new Uri(string.Format("http://<SERVERNAME_IP>:8080/messages/kafka/get?topic={0}&key={1}&message={2}", topic, key,json));

        pipe.Send(uri.PathAndQuery);
        GetResponse(uri, (x) =>
        {
            Console.WriteLine(x);
            Console.ReadLine();
        });

    }

    public static void GetResponse(Uri uri, Action<string> callback)
    {
        WebClient wc = new WebClient();
        wc.OpenReadCompleted += (o, a) =>
        {
            if (callback != null)
            {
                DataContractJsonSerializer ser = new DataContractJsonSerializer(typeof(String));
                callback(ser.ReadObject(a.Result).ToString());
            }
        };
        wc.OpenReadAsync(uri);

    }
}

public class RatePlanInventory
{
    public int SKUGroupID { get; set; }
    public int RatePlanID { get; set; }
    public String StayDate { get; set; }
    public int LogSeqNbr { get; set; }
    public int SupplierLogSeqNbr { get; set; }
    public int InventoryCntAvailable { get; set; }
    public int InventoryCntSold { get; set; }
    public String ChangeRequestID { get; set; }
    public String UpdateDate { get; set; }
    public int UpdateTPID { get; set; }
    public int UpdateTUID { get; set; }
    public string ChangeType { get; set; }

}

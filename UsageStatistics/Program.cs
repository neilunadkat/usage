using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Appacitive.Net45;
using Appacitive.Sdk;
using MySql.Data;
using MySql.Data.MySqlClient;
using Environment = Appacitive.Sdk.Environment;
using System.Net;
using System.Net.Mail;

namespace UsageStatistics
{
    class Program
    {
        static void Main(string[] args)
        {
            
            string startD = string.Empty;
            string endD = string.Empty;
            var key = string.Empty;
            
            //Gets the start date, end date and API key if sent as arguments
            if (args.Length > 0)
            {

                startD = args[0];
                endD = args[1];
                key = args.Length > 2 ? args[2] : string.Empty;

            }

            var startDate = string.IsNullOrEmpty(startD) ? DateTime.Today.AddDays(-1) : DateTime.Parse(startD);
            var endDate = string.IsNullOrEmpty(startD) ? startDate.AddDays(1) : DateTime.Parse(endD);
            var appInfoDictionary = new Dictionary<DateTime, List<AppInfo>>();
            var dataSource = new DataSource();
            var timer = startDate;
            try
            {
                while (timer < endDate)
                {
                    //Gets the tables to query on
                    var dict = dataSource.GetTables(timer);
                    dict.Keys.ToList().ForEach(d =>
                        {
                            var tables = dict[d];
                            tables.ForEach(table =>
                                {
                                    var appId = long.Parse(table.Substring(table.LastIndexOf("app") + 3));

                                    var appInfo = dataSource.GetAppInfo(appId, table, d);
                                    if (appInfo != null)
                                    {
                                        if (appInfoDictionary.ContainsKey(d) == false)
                                            appInfoDictionary[d] = new List<AppInfo>();
                                        appInfoDictionary[d].Add(appInfo);
                                    }
                                });
                        });
                    timer = timer.AddDays(1);
                }
            
            var appacitive = new AggregateStorage(startDate, endDate, appInfoDictionary, key);
            appacitive.Start();
            
            ////DataSource.DayFormat(DateTime.UtcNow);
            Console.WriteLine("press");
            Console.ReadKey(true);
            }
            catch (Exception ex)
            {
                Mailer.SendErrorMessage(startDate, ex);
            }
        }

    }

    public class AppInfo
    {
        public string ApplicationId;

        public string ApplicationName;

        public string AccountName;
         
        public long AccountId;

        public string Email;

        public string Username;

        public decimal TotalApiCalls;

        public decimal TotalArticleServiceSalls;

        public decimal TotalConnectionServiceCalls;

        public decimal TotalUserServiceCalls;

        public decimal TotalPushServiceCalls;

        public decimal TotalSearchServiceCalls;

        public decimal TotalHttpDeleteCalls;

        public decimal TotalHttpGetCalls;

        public decimal TotalHttpPostCalls;

        public decimal TotalHttpPutCalls;

        public decimal SumOfDurationOfDeleteCalls;

        public decimal SumOfDurationOfPostCalls;

        public decimal SumOfDurationOfGetCalls;

        public decimal SumOfDurationPfPutCalls;

        public DateTime Day;

        public decimal TotalFileServiceCalls;

        public string Sandbox_DeploymentId;

        public string Live_DeploymentId;
    }

    public class DataSource
    {
        private MySqlConnection _connection;
        private string _server;
        private string _database;
        private string _uid;
        private string _password;
        public static int retryCount = 0;
        //Constructor
        public DataSource()
        {
            Initialize();
        }

        //Initialize values
        private void Initialize()
        {

            _server = "{MySql Server}";
            _database = "{Storage Db}";
            _uid = "{User Id}";
            _password = "{PWD}";

            var connectionString = "SERVER=" + _server + ";" + "DATABASE=" +
                                      _database + ";" + "UID=" + _uid + ";" + "PASSWORD=" + _password + ";";

            _connection = new MySqlConnection(connectionString);
        }

        private bool OpenConnection()
        {
            try
            {
                if (this._connection.State != ConnectionState.Open)
                {
                    this._connection.Open();
                }
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("Cannot connect to MySql");
                Console.WriteLine(e);
                return false;
            }
        }

        private bool CloseConnection()
        {
            try
            {
                this._connection.Close();
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("Cannot close connection");
                Console.WriteLine(e);
                return false;
            }
        }

        public Dictionary<DateTime, List<string>> GetTables(DateTime date)
        {
            var timer = date;
            var tables = new Dictionary<DateTime, List<string>>();

            var weekNum = (int)Math.Ceiling(timer.Day / 7.0);
            var month = timer.Month;
            var year = timer.Year;

            var tableType = string.Format("week{0}-month{1}-year{2}-app%", weekNum, month, year);
            var query = "call spGetTables('" + tableType + "')";
            tables[timer] = (this.GetTableQuery(query));

            return tables;
        }

        private List<string> GetTableQuery(string query)
        {
            var tables = new List<string>();
            if (this.OpenConnection() == true)
            {
                MySqlCommand cmd = new MySqlCommand(query, this._connection);
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    tables.Add((string)reader["table_name"]);
                    Console.WriteLine(reader["table_name"]);
                }
                this.CloseConnection();
            }
            return tables;
        }

        private DataSet GetAppInfoQuery(string query)
        {
            var tables = new List<string>();
            if (this.OpenConnection() == true)
            {
                MySqlCommand cmd = new MySqlCommand(query, this._connection);
                var adapter = new MySqlDataAdapter(cmd);
                var ds = new DataSet();
                adapter.Fill(ds);

                this.CloseConnection();
                return ds;
            }
            return null;
        }

        public AppInfo GetAppInfo(long appId, string tableName, DateTime day)
        {
            var nextDay = day.AddDays(1);
            AppInfo appInfo = new AppInfo();
            var query = string.Format("call spGetAppUsageInfo('{0}','{1}',{2},'{3}');", DayFormat(day), DayFormat(nextDay), appId, tableName);
            var dataSet = this.GetAppInfoQuery(query);
            appInfo.TotalApiCalls = (long)dataSet.Tables[0].Rows[0]["count"];

            if (appInfo.TotalApiCalls > 0)
            {
                appInfo.Day = day;
                var serviceTable = dataSet.Tables.Count > 1 ? dataSet.Tables[1] : null;
                var httpMethodTable = dataSet.Tables.Count > 2 ? dataSet.Tables[2] : null;
                var accountInfo = dataSet.Tables.Count > 3 ? dataSet.Tables[3] : null;
                var deploymentInfo = dataSet.Tables.Count > 4 ? dataSet.Tables[4] : null;

                if (accountInfo != null && accountInfo.Rows.Count > 0)
                {
                    var accountRow = accountInfo.Rows[0];
                    appInfo.ApplicationId = appId.ToString();
                    appInfo.ApplicationName = (string)accountRow["appName"];
                    var username = (string)accountRow["userid"];
                    var splitUsername = username.Split(new[] { "_!..._" }, StringSplitOptions.None);
                    appInfo.AccountName = splitUsername[0];
                    appInfo.Email = splitUsername[1];
                    appInfo.Username = username;
                    appInfo.AccountId = (long)accountRow["accId"];

                    if (serviceTable != null)
                    {
                        for (int i = 0; i < serviceTable.Rows.Count; i++)
                        {
                            var row = serviceTable.Rows[i];
                            if (row["service"] != DBNull.Value)
                            {
                                switch (((string)row["service"]).ToLower())
                                {
                                    case "articleservice":

                                        appInfo.TotalArticleServiceSalls = (long)row["count"];
                                        break;
                                    case "connectionservice":
                                        appInfo.TotalConnectionServiceCalls = (long)row["count"];
                                        break;
                                    case "userservice":
                                        appInfo.TotalUserServiceCalls = (long)row["count"];
                                        break;
                                    case "pushservice":
                                        appInfo.TotalPushServiceCalls = (long)row["count"];
                                        break;
                                    case "search":
                                        appInfo.TotalSearchServiceCalls = (long)row["count"];
                                        break;
                                    case "fileservice":
                                        appInfo.TotalFileServiceCalls = (long)row["count"];
                                        break;
                                }
                            }
                        }
                    }

                    if (httpMethodTable != null)
                    {
                        for (int i = 0; i < httpMethodTable.Rows.Count; i++)
                        {
                            var row = httpMethodTable.Rows[i];
                            if (row["httpmethod"] != DBNull.Value)
                            {
                                switch (((string)row["httpmethod"]).ToLower())
                                {
                                    case "get":
                                        appInfo.SumOfDurationOfGetCalls = (decimal)row["sum"];
                                        appInfo.TotalHttpGetCalls = (long)row["count"];
                                        break;
                                    case "put":
                                        appInfo.SumOfDurationPfPutCalls = (decimal)row["sum"];
                                        appInfo.TotalHttpPutCalls = (long)row["count"];
                                        break;
                                    case "post":
                                        appInfo.SumOfDurationOfPostCalls = (decimal)row["sum"];
                                        appInfo.TotalHttpPostCalls = (long)row["count"];
                                        break;
                                    case "delete":
                                        appInfo.SumOfDurationOfDeleteCalls = (decimal)row["sum"];
                                        appInfo.TotalHttpDeleteCalls = (long)row["count"];
                                        break;
                                }
                            }
                        }
                    }

                    if (deploymentInfo != null)
                    {
                        for (int i = 0; i < deploymentInfo.Rows.Count; i++)
                        {
                            var row = deploymentInfo.Rows[i];
                            if (row["deploymentName"] != DBNull.Value)
                            {
                                var name = (string)row["deploymentName"];
                                var did = ((long)row["deploymentId"]).ToString();
                                if (name.ToLower().Contains("sandbox"))
                                {
                                    appInfo.Sandbox_DeploymentId = did;
                                }
                                else
                                    appInfo.Live_DeploymentId = did;
                            }
                        }
                    }
                    return appInfo;
                }

            }
            return null;
        }

        public static string DayFormat(DateTime day)
        {
            var d = day.Day;
            var m = day.Month;
            var y = day.Year;
            return "" + y + "-" + (m <= 9 ? "0" + m.ToString(CultureInfo.InvariantCulture) : m.ToString(CultureInfo.InvariantCulture)) + "-" + (d <= 9 ? "0" + d.ToString(CultureInfo.InvariantCulture) : d.ToString(CultureInfo.InvariantCulture));
        }

        public Dictionary<long, Dictionary<string, long>> GetAppDeploymentInfo()
        {
            var appInfo = new Dictionary<long, Dictionary<string, long>>();
            var query = "call spGetAllAppInfo()";
            var dataSet = GetAppInfoQuery(query);

            var table = dataSet.Tables[0];
            if (table != null)
            {
                for (int i = 0; i < table.Rows.Count; i++)
                {
                    var row = table.Rows[i];
                    var appId = (long)row["AppId"];
                    if (appInfo.ContainsKey(appId) == false)
                    {
                        appInfo[appId] = new Dictionary<string, long>(StringComparer.CurrentCultureIgnoreCase);
                    }
                    var depId = (long)row["DID"];
                    var dName = (string)row["DName"];

                    if (dName.ToLower().Contains("sandbox"))
                    {
                        appInfo[appId]["sandbox"] = depId;
                    }
                    else
                        appInfo[appId]["live"] = depId;
                }
            }
            return appInfo;
        }

        public Dictionary<string, long> GetAccountInfo()
        {
            var accInfo = new Dictionary<string, long>(StringComparer.InvariantCultureIgnoreCase);
            var query = "call spGetAccountInfo()";
            var dataSet = GetAppInfoQuery(query);

            var table = dataSet.Tables[0];
            if (table != null)
            {
                for (int i = 0; i < table.Rows.Count; i++)
                {
                    var row = table.Rows[i];
                    var accName = (string)row["name"];
                    var id = (long)row["id"];
                    accInfo[accName] = id;

                }
            }
            return accInfo;
        }

    }
        
    public class AggregateStorage
    {
        private string _url;
        private string _apiKey;
        private readonly Dictionary<string, string> _monthArticleIds;
        private readonly Dictionary<DateTime, string> _dailyArticleIds;
        private readonly Dictionary<string, string> _appMonthlyArticleIds;
        private readonly DateTime _startDate;
        private readonly DateTime _endDate;
        private readonly Dictionary<DateTime, List<AppInfo>> _appsInfo;
        public bool Finished;

        public static readonly string[] _months = new []{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};

        public AggregateStorage() 
        {
            var key = "{UsageTracker APIKey}";
            App.Initialize(WindowsRT.Host, key, Environment.Live);
            App.Debug.Out = Console.Out;
            //App.Debug.IsEnabled = true;
        }

        public AggregateStorage(DateTime startDate,DateTime endDate, Dictionary<DateTime, List<AppInfo>> apps,string apiKey )
        {
           
            var key = string.IsNullOrEmpty(apiKey) ? @"{USAGETRACKER APIKEY}" : apiKey;
            App.Initialize(WindowsRT.Host, key, Environment.Live);
            App.Debug.Out = Console.Out;
            //App.Debug.IsEnabled = true;
            _monthArticleIds = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
            _dailyArticleIds = new Dictionary<DateTime, string>();
            _appMonthlyArticleIds = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
            _startDate = startDate;
            _endDate = endDate;
            _appsInfo = apps;
        }

        public async void Start()
        {
            await Setup();
            await StoreAppsInfo();

        }

        private  async Task Setup()
        {
            var timer = _startDate;
            while (timer < _endDate)
            {
                var month = _months[timer.Month -1] + " " + timer.Year;
                if (_monthArticleIds.ContainsKey(month) == false)
                {
                    await SetMonthlyUsage(month);
                }
                await SetDailyUsage(timer);
                timer = timer.AddDays(1);
            }
        }

        private  async Task SetDailyUsage(DateTime day)
        {
            var articles = await Articles.FindAllAsync("dailyusage", Query.Property("day").IsEqualToDate(day).AsString());
            string id = "";
            if (articles.Count == 0)
            {
                dynamic article = new Article("dailyusage");
                article.day = DataSource.DayFormat(day);
                await article.SaveAsync();
                var created = article as Article;
                if (created != null)
                {
                    _dailyArticleIds[day] = created.Id;
                }


            }
            else
            {
                _dailyArticleIds[day] = articles[0].Id;
            }
        }

        private  async Task SetMonthlyUsage(string month)
        {
            var articles = await Articles.FindAllAsync("monthlyusage", Query.Property("month").IsEqualTo(month).AsString());
            string id = "";
            if (articles.Count == 0)
            {
                dynamic article = new Article("monthlyusage");
                article.month = month;
                await article.SaveAsync();
                var created = article as Article;
                if (created != null)
                {
                    _monthArticleIds[month] = created.Id;
                }


            }
            else
            {
                _monthArticleIds[month] = articles[0].Id;
            }
        }

        private static string GetAppMonthId(string appId, string month)
        {
            return string.Format("{0}_{1}", appId, month);
        }

        private async Task StoreAppsInfo()
        {
            var i = 0;
            this._appsInfo.Keys.ToList().ForEach(d =>
                {
                    var apps = _appsInfo[d];
                    apps.ForEach(async appInfo =>
                        {
                            try
                            {
                                if (i > 1)
                                    Thread.Sleep(100);
                                i++;
                                Console.WriteLine("i ="+ i);
                                var id = await CreateAppArticle(appInfo);
                                var month = string.Format("{0} {1}", _months[d.Month - 1], d.Year);
                                await CreateAppMonthlyUsage(month, appInfo);
                                
                                await this.ConnectAppWithDaily(id, d);
                                await this.ConnectAppMonthly(id, month);
                                await this.ConnectAppToAppMonthly(id, month, appInfo.ApplicationId);
                                i--;
                             }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Exception : "+ ex);
                                Mailer.SendErrorMessage(_startDate,ex);
                            }

                        });
                });

            Mailer.SendSuccessMessage(_startDate);
            Console.WriteLine("Finished");
            
        }

        private async Task<string> CreateAppArticle(AppInfo appInfo)
        {
            dynamic obj = new Article("appdayusage");
            Convert(obj,appInfo);
            await obj.SaveAsync();
            var created = obj as Article;
            return created.Id;

        }

        private async Task ConnectAppWithDaily(string appDailyArticleId, DateTime day)
        {
            var dailyId = this._dailyArticleIds[day];
            var conn = Connection
                .New("daily")
                .FromExistingArticle("dailyusage", dailyId)
                .ToExistingArticle("appdayusage", appDailyArticleId);

            await conn.SaveAsync();
            if (string.IsNullOrEmpty(conn.Id))
            {
                Console.WriteLine("Error in creating daily connection");
            }
        }

        private async Task ConnectAppMonthly(string appDailyArticleId, string month)
        {
            var monthlyId = this._monthArticleIds[month];
            var conn = Connection
                .New("monthly")
                .FromExistingArticle("monthlyusage1", monthlyId)
                .ToExistingArticle("appdayusage2", appDailyArticleId);

            await conn.SaveAsync();
            if (string.IsNullOrEmpty(conn.Id))
            {
                Console.WriteLine("Error in creating monthly connection");
            }
        }

        private async Task CreateAppMonthlyUsage(string month, AppInfo appInfo)
        {
                if (this._appMonthlyArticleIds.ContainsKey(GetAppMonthId(appInfo.ApplicationId, month)) == false)
                {
                    this._appMonthlyArticleIds[GetAppMonthId(appInfo.ApplicationId, month)] = "0";
                    var query = BooleanOperator.And(new[]
                    {
                        Query.Property("application_id").IsEqualTo(appInfo.ApplicationId),
                        Query.Property("month").IsEqualTo(month)
                    })
                                                 .AsString()
                          ;
                    var appMonthly = await Articles.FindAllAsync("appmonthlyusage", query);
                    if (appMonthly.Count == 0)
                    {
                        dynamic obj = new Article("appmonthlyusage");
                        obj.application_id = appInfo.ApplicationId;
                        obj.application_name = appInfo.ApplicationName;
                        obj.account_name = appInfo.AccountName;
                        obj.email = appInfo.Email;
                        obj.application_name = appInfo.ApplicationName;
                        obj.month = month;
                        obj.sandbox_deploymentid = appInfo.Sandbox_DeploymentId;
                        obj.live_deploymentid = appInfo.Live_DeploymentId;
                        obj.account_id = appInfo.AccountId;
                        await obj.SaveAsync();
                        var created = obj as Article;
                        if (string.IsNullOrEmpty(created.Id) == false)
                        {
                            this._appMonthlyArticleIds[GetAppMonthId(appInfo.ApplicationId, month)] = created.Id;
                        }
                    }
                    else 
                    {
                        this._appMonthlyArticleIds[GetAppMonthId(appInfo.ApplicationId, month)] = appMonthly[0].Id;
                    }
                }
                else if (this._appMonthlyArticleIds[GetAppMonthId(appInfo.ApplicationId, month)] == "0") 
                {
                    while (this._appMonthlyArticleIds[GetAppMonthId(appInfo.ApplicationId, month)] == "0") 
                    {
                        Thread.Sleep(50);
                    }
                }
        }

        private async Task ConnectAppToAppMonthly(string appDailyArticleId, string month,string appId)
        {

            var conn = Connection
                .New("appmonthly")
                .FromExistingArticle("appmonthlyusage", this._appMonthlyArticleIds[GetAppMonthId(appId, month)])
                .ToExistingArticle("appdayusage", appDailyArticleId);

            await conn.SaveAsync();
            if (string.IsNullOrEmpty(conn.Id))
            {
                Console.WriteLine("Error in creating app monthly");
            }
        }

        private async Task<List<string>> GetArticlesForApp(string appId,string type) {
            var articleIds = new List<string>();
            var articles = await Articles.FindAllAsync(type, Query.Property("application_id").IsEqualTo(appId).AsString(), pageSize: 200, page: 1);

            if (articles.Count > 0) 
            {
                articles.ForEach(article => articleIds.Add(article.Id));
            }

            return articleIds;

        }

        private async Task<List<string>> GetArticlesForAcc(string accName, string type)
        {
            var articleIds = new List<string>();
            var articles = await Articles.FindAllAsync(type, Query.Property("account_name").IsEqualTo(accName).AsString(), pageSize: 200, page: 1);

            if (articles.Count > 0)
            {
                articles.ForEach(article => articleIds.Add(article.Id));
            }

            return articleIds;

        }

        private static void Convert(dynamic obj, AppInfo appInfo)
        {
            obj.application_id = appInfo.ApplicationId ;
            obj.application_name = appInfo.ApplicationName ;
            obj.account_name = appInfo.AccountName ;
            obj.account_id = appInfo.AccountId;
            obj.email = appInfo.Email ;
            obj.username = appInfo.Username ;
            obj.totalapicalls = appInfo.TotalApiCalls ;
            obj.totalarticleservicecalls = appInfo.TotalArticleServiceSalls ;
            obj.totalconnectionservicecalls = appInfo.TotalConnectionServiceCalls ;
            obj.totaluserservicecalls = appInfo.TotalUserServiceCalls ;
            obj.totalpushservicecalls = appInfo.TotalPushServiceCalls ;
            obj.totalsearchservicecalls = appInfo.TotalSearchServiceCalls ;
            obj.totalhttpdeletecalls = appInfo.TotalHttpDeleteCalls ;
            obj.totalhttpgetcalls = appInfo.TotalHttpGetCalls ;
            obj.totalhttppostcalls = appInfo.TotalHttpPostCalls ;
            obj.totalhttpputcalls = appInfo.TotalHttpPutCalls ;
            obj.sumofdurationofdeletecalls = appInfo.SumOfDurationOfDeleteCalls ;
            obj.sumofdurationofpostcalls = appInfo.SumOfDurationOfPostCalls ;
            obj.sumofdurationofgetcalls = appInfo.SumOfDurationOfGetCalls ;
            obj.sumofdurationofputcalls = appInfo.SumOfDurationPfPutCalls ;
            obj.day = DataSource.DayFormat(appInfo.Day) ;
            obj.totalfileservicecalls = appInfo.TotalFileServiceCalls;
            obj.sandbox_deploymentid = appInfo.Sandbox_DeploymentId;
            obj.live_deploymentid = appInfo.Live_DeploymentId;
        }
    }

    public class Mailer
    {
        private static NetworkCredential cred = new NetworkCredential("{Sender Email}", "{PWD}");
        private static SmtpClient client = new SmtpClient("smtp.gmail.com", 25);
        private static string usagePageURL = "{Usage page url}";
        private static List<string> successRecipients = new string[] {"{MAILID}" }.ToList();
        private static List<string> errorRecipients = new string[] {"{MAILID}" }.ToList();

        public static void SendSuccessMessage(DateTime d) 
        {
            MailMessage msg = new MailMessage();
            successRecipients.ForEach(m => msg.To.Add(m));
            msg.From = new MailAddress("{MAILID}");
            msg.Subject = string.Format("API Usage for yesterday");
            msg.Body = string.Format("API usage for {0} {1} {2} {3} has been updated.\nYou can find the readings here {4}"
                ,d.DayOfWeek.ToString()
                ,d.Day
                ,AggregateStorage._months[d.Month - 1]
                ,d.Year
                ,usagePageURL
                );

            client.Credentials = cred;
            client.EnableSsl = true;
            //client.Send(msg);
        }
        public static void SendErrorMessage(DateTime d, Exception e) 
        {
            MailMessage msg = new MailMessage();
            var appException = e as AppacitiveException;
            errorRecipients.ForEach(m => msg.To.Add(m));
            msg.From = new MailAddress("{MAILID}");
            msg.Subject = string.Format("Error while getting API Usage for yesterday");
            msg.Body = string.Format(" Error while getting API usage for {0} {1} {2} {3}. \n Here it is: \n{4} \n RefID : {5}"
                , d.DayOfWeek.ToString()
                , d.Day
                , AggregateStorage._months[d.Month - 1]
                , d.Year
                , appException.Message
                ,appException.ReferenceId
                );

            client.Credentials = cred;
            client.EnableSsl = true;
            client.Send(msg);
        }
    }
}

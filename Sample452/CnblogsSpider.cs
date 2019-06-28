using System;
using System.Threading.Tasks;
using DotnetSpider.Core;
using DotnetSpider.Data;
using DotnetSpider.Data.Parser;
using DotnetSpider.Data.Storage;
using DotnetSpider.Downloader;
using DotnetSpider.MessageQueue;
using DotnetSpider.Scheduler;
using DotnetSpider.Statistics;
using Microsoft.Extensions.Logging;

namespace DotnetSpider.Sample.samples
{
    public class CnblogsSpider : Spider
    {
        protected override void Initialize()
        {		
            NewGuidId();
            Scheduler = new QueueDistinctBfsScheduler();
            Speed = 2;
          //  Depth = 3;
			EmptySleepTime = 300;
            DownloaderSettings.Type = DownloaderType.HttpClient;
            AddDataFlow(new CnblogsDataParser()).AddDataFlow(new JsonFileStorage());
            AddRequests("http://www.cnblogs.com/");
        }

        class CnblogsDataParser : DataParser
        {
            public CnblogsDataParser()
            {
                CanParse = DataParserHelper.CanParseByRegex("cnblogs\\.com");
                QueryFollowRequests = DataParserHelper.QueryFollowRequestsByXPath("//div[@class='pager']/a[contains(text(),'Next')]");
            }

            protected override Task<DataFlowResult> Parse(DataFlowContext context)
            {
                context.AddItem("URL", context.Response.Request.Url);
                context.AddItem("Title", context.GetSelectable().XPath(".//title").GetValue());
                return Task.FromResult(DataFlowResult.Success);
            }
        }

        public CnblogsSpider(IDynamicMessageQueue dmq, IMessageQueue mq, IStatisticsService statisticsService, ISpiderOptions options, ILogger<Spider> logger, IServiceProvider services) 
			: base(dmq, mq, statisticsService, options, logger, services)
        {
        }
    }
}
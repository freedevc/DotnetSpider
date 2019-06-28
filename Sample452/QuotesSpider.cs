using DotnetSpider;
using DotnetSpider.Core;
using DotnetSpider.Data;
using DotnetSpider.Data.Parser;
using DotnetSpider.Data.Storage;
using DotnetSpider.Downloader;
using DotnetSpider.MessageQueue;
using DotnetSpider.Scheduler;
using DotnetSpider.Statistics;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sample452
{
	public class QuotesSpider:Spider
	{
		protected override void Initialize()
		{
			NewGuidId();
			Scheduler = new QueueDistinctBfsScheduler();
			Speed = 1;
			Depth = 3;
			DownloaderSettings.Type = DownloaderType.HttpClient;
			AddDataFlow(new QuotesDataParser()).AddDataFlow(new ConsoleStorage());
			AddRequests("http://quotes.toscrape.com/");
		}

		class QuotesDataParser : DataParser
		{
			public QuotesDataParser()
			{
				//CanParse = DataParserHelper.CanParseByRegex("cnblogs\\.com");
				QueryFollowRequests = DataParserHelper.QueryFollowRequestsByXPath("//li[@class='next']");
			}

			protected override Task<DataFlowResult> Parse(DataFlowContext context)
			{
				context.AddItem("URL", context.Response.Request.Url);
				context.AddItem("Quotes", context.GetSelectable().XPath("//div[@class='quote']").GetValues());
				return Task.FromResult(DataFlowResult.Success);
			}
		}

		public QuotesSpider(IDynamicMessageQueue dmq, IMessageQueue mq, IStatisticsService statisticsService,
			ISpiderOptions options, ILogger<Spider> logger, IServiceProvider services) : base(dmq,mq, statisticsService, options, logger, services)
		{
		}
	}
}

using DotnetSpider;
using DotnetSpider.Core;
using DotnetSpider.Data;
using DotnetSpider.Data.Parser;
using DotnetSpider.Data.Storage;
using DotnetSpider.Downloader;
using DotnetSpider.MessageQueue;
using DotnetSpider.Sample.samples.HtmlFileStorage;
using DotnetSpider.Scheduler;
using DotnetSpider.Statistics;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
namespace Sample452
{
	public class VnexpressSpider : Spider
	{
		public static Task Run()
		{
			//var spider = Create<VnexpressSpider>();
			var builder = new SpiderBuilder();
			builder.AddSerilog();
			builder.ConfigureAppConfiguration();
			builder.UseStandalone();
			builder.AddSpider<VnexpressSpider>();
			builder.Services.AddSingleton<IDynamicMessageQueue, InMemoryMessageQueue>((s)=> null);
			//builder.Services.AddSingleton<IDynamicMessageQueue>(null as IDynamicMessageQueue);
			var factory = builder.Build();
			var spider = factory.Create<VnexpressSpider>();
			return spider.RunAsync();
		}
		protected override void Initialize()
		{
			NewGuidId();
			Scheduler = new QueueDistinctBfsScheduler();
			Speed = 5;
			Depth = 2;
			PageLimit = 2;
			DownloaderSettings.Type = DownloaderType.HttpClient;
			AddDataFlow(new VnexpressItemLinksDataParser()).AddDataFlow(new HtmlFileStorage()).AddDataFlow(new VnexpressPaginationDataParser());
			AddRequests(new string[] { "https://vnexpress.net/kinh-doanh" });
			//AddRequests(new string[] { "https://vnexpress.net/kinh-doanh", "https://vnexpress.net/the-gioi", "https://vnexpress.net/goc-nhin", "https://vnexpress.net/the-thao", "https://vnexpress.net/phap-luat", "https://vnexpress.net/giao-duc" });
		}

		protected override Task OnExiting(IServiceProvider serviceProvider)
		{
			//base.OnExiting().ConfigureAwait(false).GetAwaiter();
			serviceProvider.GetService<IDownloadCenter>()?.StopAsync(default).ConfigureAwait(false).GetAwaiter();
			serviceProvider.GetService<IDownloaderAgent>()?.StopAsync(default).ConfigureAwait(false).GetAwaiter();
			serviceProvider.GetService<IStatisticsCenter>()?.StopAsync(default).ConfigureAwait(false).GetAwaiter();
			return Task.CompletedTask;
		}
		class VnexpressItemLinksDataParser : DataParser
		{
			public VnexpressItemLinksDataParser()
			{
			//	CanParse = DataParserHelper.CanParseByRegex("vnexpress\\.com");
				QueryFollowRequests = DataParserHelper.QueryFollowRequestsByXPath(new string[] { "//article/h1[@class='title_news']/a[1]", "//article[@class='list_news']/h4[@class='title_news']/a[1]" });
			}

			protected override Task<DataFlowResult> Parse(DataFlowContext context)
			{
				context.AddItem("URL", context.Response.Request.Url);
				var item = context.GetSelectable().XPath("//h1[@class='title_news_detail mb10']").GetValue();
				if (!string.IsNullOrWhiteSpace(item))
				{
					//	context.AddItem("Vnexpress", item);
					context.AddItem("Content:", context.Response.RawText);
				}
				else
					context.ClearItems();
				return Task.FromResult(DataFlowResult.Success);
			}
		}

		//class VnexpressItemDetailDataParser : DataParser
		//{
		//	public VnexpressItemDetailDataParser()
		//	{
		//		CanParse = DataParserHelper.CanParseByRegex("vnexpress\\.com");
		//	//	QueryFollowRequests = DataParserHelper.QueryFollowRequestsByXPath(new string[] { "//h4[@class='title_news']/a", "//p[@id='pagination']/a[@class='next']" });
		//	}

		//	protected override Task<DataFlowResult> Parse(DataFlowContext context)
		//	{
		//		context.AddItem("URL", context.Response.Request.Url);
		//		var item = context.GetSelectable().XPath("//h1[@class='title_news_detail mb10']").GetValue();
		//		if (!string.IsNullOrWhiteSpace(item))
		//		{
		//			//	context.AddItem("Vnexpress", item);
		//			context.AddItem("Content:", context.Response.RawText);
		//		}
		//		else
		//			context.ClearItems();
		//		return Task.FromResult(DataFlowResult.Success);
		//	}
		//}
		class VnexpressPaginationDataParser : DataParser
		{
			public VnexpressPaginationDataParser()
			{
			//	CanParse = DataParserHelper.CanParseByRegex("cnblogs\\.com");
				//	QueryFollowRequests = DataParserHelper.QueryFollowRequestsByXPath(new string[] { "//h4[@class='title_news']/a", "//p[@id='pagination']/a[@class='next']" });
			}

			protected override Task<DataFlowResult> Parse(DataFlowContext context)
			{
				var nextPageUrl = context.GetSelectable().XPath("//p[@id='pagination']/a[@class='next']").Links().GetValue();
				if (!string.IsNullOrWhiteSpace(nextPageUrl))
				{
					var followRequest = CreateFromRequest(context, nextPageUrl);
					followRequest.PageIndex = context.Response.Request.PageIndex + 1;
					if (CanParse == null || CanParse(followRequest))
					{
						context.FollowRequests.Add(followRequest);
					}
				}
				return Task.FromResult(DataFlowResult.Success);
			}
		}

		public VnexpressSpider( IMessageQueue mq, IStatisticsService statisticsService,
			ISpiderOptions options, ILogger<Spider> logger, IServiceProvider services) : base(null, mq, statisticsService, options, logger, services)
		{
		}
		//public VnexpressSpider(IDynamicMessageQueue dmq, IMessageQueue mq, IStatisticsService statisticsService,
		//	ISpiderOptions options, ILogger<Spider> logger, IServiceProvider services) : base(dmq, mq, statisticsService, options, logger, services)
		//{
		//}
	}
}

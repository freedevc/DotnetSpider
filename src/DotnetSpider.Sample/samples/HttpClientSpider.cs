using DotnetSpider;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using DotnetSpider.Scheduler;
using DotnetSpider.Downloader;
using DotnetSpider.Sample.Parsers;
using DotnetSpider.Statistics;
using DotnetSpider.MessageQueue;
using DotnetSpider.Core;

namespace DotnetSpider.Sample
{
	

	public class HttpClientSpider : Spider
	{
		private ProjectDefinition _definition;
		public static Task Run()
		{
			//var spider = Create<VnexpressSpider>();
			var builder = new SpiderBuilder();
			builder.AddSerilog();
			builder.Services.AddSingleton<IProxyValidator, FakeProxyValidator>();
			//builder.Services.AddSingleton<IProxyValidator, DefaultProxyValidator>();
			builder.ConfigureAppConfiguration(null,args: new string[] { "/ProxySupplyUrl=http://localhost:52445/api/proxies" },true);
			builder.UseStandalone();
			var settings = new ProjectDefinition()
			{
				ProjectName = "Vnexpress Spider",
				Site = "Vnexpress/Kinh Doanh",
				ItemUrlsSelector = "//article/h1[@class='title_news']/a[1];//article[@class='list_news']/h4[@class='title_news']/a[1]",
				Urls = "https://vnexpress.net/kinh-doanh",
				FileStorage = @"P:\Neil.Test\Spider Storage\Vnexpress",
				FileFormat = "*.json",
				PageLimit = 4,
				Deepth = 2,
				NextPageSelector ="//p[@id='pagination']/a[@class='next']",
				NumberOfConcurrentRequests =5,
				Mapping = new ItemMapping
				{
					ItemCssSelector = "//section[@id='left_calculator']",
					Mapping = new FieldMapping[]
					{
							new FieldMapping{ Field = "Title", CssSelector = "//h1[@class='title_news_detail mb10']"},
							new FieldMapping{ Field = "Description", CssSelector = "//p[@class='description']"},
					}
				}
			};
			builder.Services.AddSingleton<ProjectDefinition>(settings);
			builder.AddSpider<HttpClientSpider>();
			//	builder.Services.AddSingleton<IDynamicMessageQueue, InMemoryMessageQueue>((s)=> null);
			//builder.Services.AddSingleton<IDynamicMessageQueue,InMemoryMessageQueue>();
			builder.UseDynamicMessageQueue();
			var factory = builder.Build();
			var spider = factory.Create<HttpClientSpider>();
			return spider.RunAsync();
		}
		protected override void Initialize()
		{
			NewGuidId();
			Scheduler = new QueueDistinctBfsScheduler();
			if (_definition.NumberOfConcurrentRequests > 0)
				Speed = _definition.NumberOfConcurrentRequests;
			if (_definition.Deepth.GetValueOrDefault() > 0)
				Depth = _definition.Deepth.Value;
			if (_definition.PageLimit.GetValueOrDefault() > 0)
				PageLimit = _definition.PageLimit.Value;
			//	DownloaderSettings.
			DownloaderSettings.Type = DownloaderType.HttpClient;
			DownloaderSettings.UseProxy = true;
			AddDataFlow(new SimpleItemDataParser(_definition.ItemUrlsSelector, _definition.Mapping))
			.AddDataFlow(new HtmlFileStorage(_definition));
			if (!string.IsNullOrWhiteSpace(_definition.NextPageSelector))
				AddDataFlow(new SimplePaginationDataParser(_definition.NextPageSelector));
			AddRequests(_definition.Urls.Split(';', StringSplitOptions.RemoveEmptyEntries));
			//AddRequests(new string[] { "https://vnexpress.net/kinh-doanh" });
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

		//public VnexpressSpider( IMessageQueue mq, IStatisticsService statisticsService,
		//	ISpiderOptions options, ILogger<Spider> logger, IServiceProvider services) : base(null, mq, statisticsService, options, logger, services)
		//{
		//}
		public HttpClientSpider(ProjectDefinition definition, IDynamicMessageQueue dmq, IMessageQueue mq, IStatisticsService statisticsService,
			ISpiderOptions options, ILogger<Spider> logger, IServiceProvider services) : base(dmq, mq, statisticsService, options, logger, services)
		{
			_definition = definition;
		}
	}

}

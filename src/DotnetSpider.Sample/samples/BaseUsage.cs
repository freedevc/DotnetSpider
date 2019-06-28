using System.Threading.Tasks;
using DotnetSpider.Data;
using DotnetSpider.Data.Parser;
using DotnetSpider.Data.Storage;
using DotnetSpider.Downloader;
using DotnetSpider.Scheduler;

namespace DotnetSpider.Sample.samples
{
	public class BaseUsage
	{
		public static Task Run()
		{
			var builder = new SpiderBuilder();
			builder.AddSerilog();
			builder.ConfigureAppConfiguration();
			builder.UseStandalone();
			
			builder.AddSpider<EntitySpider>();
			var provider = builder.Build();

			var spider = provider.Create<Spider>();
			spider.Scheduler = new QueueBfsScheduler();
			spider.NewGuidId(); // 设置任务标识
			spider.Name = "博客园全站采集"; // 设置任务名称
			spider.Speed = 5; // 设置采集速度, 表示每秒下载多少个请求, 大于 1 时越大速度越快, 小于 1 时越小越慢, 不能为0.
		//	spider.Depth = 3; // 设置采集深度
			spider.DownloaderSettings.Type = DownloaderType.HttpClient; // 使用普通下载器, 无关 Cookie, 干净的 HttpClient
			spider.AddDataFlow(new CnblogsDataParser()).AddDataFlow(new ConsoleStorage());
			spider.AddRequests("http://www.cnblogs.com/"); // 设置起始链接
			return spider.RunAsync(); // 启动
		}

		class CnblogsDataParser : DataParser
		{
			public CnblogsDataParser()
			{
				CanParse = DataParserHelper.CanParseByRegex("cnblogs\\.com");
				//	QueryFollowRequests = DataParserHelper.QueryFollowRequestsByXPath(".");
				QueryFollowRequests = DataParserHelper.QueryFollowRequestsByXPath("//div[@class='pager']/a[contains(text(),'Next')]");
			}

			protected override Task<DataFlowResult> Parse(DataFlowContext context)
			{
				context.AddItem("URL", context.Response.Request.Url);
				context.AddItem("Title", context.GetSelectable().XPath(".//title").GetValue());
				
				return Task.FromResult(DataFlowResult.Success);
			}
		}
	}
}
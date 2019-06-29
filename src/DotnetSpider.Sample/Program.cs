using System;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using DotnetSpider.Sample.samples;
using Sample452;

namespace DotnetSpider.Sample
{
	class Program
	{
		static async Task Main(string[] args)
		{
			Console.Title = $"Spider: " + Process.GetCurrentProcess().Id.ToString();
			//ServicePointManager.DefaultConnectionLimit = int.MaxValue;
			//	ServicePointManager.EnableDnsRoundRobin = true;
			//ServicePointManager.ReusePort = true;
			//await BaseUsage.Run();
			//			await GithubSpider.Run();
			//Task.Run(async () =>
			//	{
			await HttpClientSpider.Run();
					//await VnexpressSpider.Run();
			//	var temp = true;
		//	});
			//Task.Run(async () =>
			//{
			//	await GithubSpider.Run();
			//});
			//await DistributedSpider.Run(); 
			Console.Read();
		}

		static Task Write(string msg)
		{
			Console.WriteLine(msg);
			return Task.CompletedTask;
		}
	}
}
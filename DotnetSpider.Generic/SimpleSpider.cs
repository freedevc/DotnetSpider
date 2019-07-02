using DotnetSpider.Core;
using DotnetSpider.Core.Pipeline;
using DotnetSpider.Core.Processor;
using DotnetSpider.Core.Scheduler;
using DotnetSpider.Downloader;
using DotnetSpider.EtechDownloader;
using DotnetSpider.Extraction;
using DotnetSpider.Proxy;
using DotnetSpider.Generic.FileService;
using DotnetSpider.Generic.Proxy;
using DotnetSpider.Generic.Extensions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DotnetSpider.Generic.docs
{
	public class GenericSpider : Spider
	{
		protected override void OnInit(params string[] arguments)
		{
			base.OnInit(arguments);

		}
		public static void Run()
		{
			//var settings = new ProjectDefinition()
			//{
			//	ProjectName = "Vnexpress Spider",
			//	Site = "Vnexpress/Kinh Doanh",
			//	ItemUrlsSelector = "//article/h1[@class='title_news']/a[1];//article[@class='list_news']/h4[@class='title_news']/a[1]",
			//	Urls = "https://vnexpress.net/kinh-doanh",
			//	DestinationFolder = @"P:\Neil.Test\Spider Storage\Vnexpress",
			//	FileFormat = "*.html",
			//	PageLimit = 4,
			//	Deepth = 2,
			//	NextPageSelector = "//p[@id='pagination']/a[@class='next']",
			//	NumberOfConcurrentRequests = 5,
			//	ExtractType = "PageSource",
			//	Mapping = new ItemMapping
			//	{
			//		//ItemCssSelector = "//section[@id='left_calculator']",
			//		//Mapping = new FieldMapping[]
			//		//{
			//		//		new FieldMapping{ Field = "Title", CssSelector = "//h1[@class='title_news_detail mb10']"},
			//		//		new FieldMapping{ Field = "Description", CssSelector = "//p[@class='description']"},
			//		//},
			//		OnlyPagesAtDepth = 2
			//	},
			//	Proxies ="127.0.0.1:11550;127.0.0.1:11551;127.0.0.1:11552" ,

			//};
			////h2[@class='message_subject']/a[@class='page-link lia-link-navigation lia-custom-event']
			var settings = new ProjectDefinition()
			{
				ProjectName = "QB Helps",
				Site = "QB/Helps",
				ItemUrlsSelector = "//a[@class='board-grid-item-title'];//a[@class='page-link lia-link-navigation lia-custom-event']",
				Urls = "https://quickbooks.intuit.com/community/Help-articles/ct-p/help-articles-us?label=QuickBooks%20Desktop",
				DestinationFolder = @"P:\Neil.Test\Spider Storage\QBHelp",
				FileFormat = "*.html",
				PageLimit = 4,
				//Deepth = 2,
				NextPageSelector = "//li[@class='lia-paging-page-next lia-component-next']/a[@rel='next']",
				NumberOfConcurrentRequests =5,
				ExtractType = "PageSource",
				Mapping = new ItemMapping
				{
					//ItemCssSelector = "//section[@id='left_calculator']",
					//Mapping = new FieldMapping[]
					//{
					//		new FieldMapping{ Field = "Title", CssSelector = "//h1[@class='title_news_detail mb10']"},
					//		new FieldMapping{ Field = "Description", CssSelector = "//p[@class='description']"},
					//},
					OnlyPagesAtDepth = 3
				},
				Proxies = "127.0.0.1:11550;127.0.0.1:11551;127.0.0.1:11552",

			};

			var fileService = new LocalFileStorageService();
			IPipeline storagePipeLine = null;
			if (string.Compare(settings.ExtractType, "PageSource", true) == 0)
				storagePipeLine = new FileStoragePipeline(fileService, settings);
			else
				storagePipeLine = new SimpleFileStoragePipeline(settings);
			Spider spider = Spider.Create(
				new QueueDuplicateRemovedScheduler(),
				new SimplePageProcessor(settings))
				.AddPipeline(storagePipeLine);
			// Start crawler
			spider.EncodingName = "UTF-8";
			//var proxyUrl = "http://localhost:52445/api/proxies";
			string[] proxies = new string[] { };
			if (!string.IsNullOrWhiteSpace(settings.Proxies))
			{
				proxies = settings.Proxies.Split(";".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
			}
			CustomHttpProxyPool pool = new CustomHttpProxyPool(new StringProxySupplier(proxies));
			pool.ProxyValidator = new FakeProxyValidator(true);
			spider.Scheduler.TraverseStrategy = TraverseStrategy.Bfs;

			spider.Downloader = new CustomHttpClientDownloader(pool, fileService, settings) { UseProxy = true };
			// this flag is important because we need the spider continue process for search result pages where we only need urls to go to detail page.
			spider.SkipTargetRequestsWhenResultIsEmpty = false;

			if (settings.Deepth.GetValueOrDefault() > 0)
				spider.Depth = settings.Deepth.Value;
			if (settings.NumberOfConcurrentRequests > 0)
				spider.ThreadNum = settings.NumberOfConcurrentRequests;
			spider.Name = settings.ProjectName;
			var startUrls = settings.Urls.Split(";".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
			foreach (var startUrl in startUrls)
			{
				var properties = new Dictionary<string, object>();
				properties.Add(Page.Depth, 1);
				properties.Add(ProjectDefinition.PageIndexKey, 1);
				spider.AddRequests(new Request(startUrl, properties));
			}
			spider.Run();
		}

		private class SimpleFileStoragePipeline : BaseFilePipeline
		{
			private ProjectDefinition _settings;
			public SimpleFileStoragePipeline(ProjectDefinition settings) : base(null)
			{
				this._settings = settings;
			}
			public override void Process(IList<ResultItems> resultItems, dynamic sender = null)
			{
				var identity = sender as IIdentity;
				var extension = "html";
				if (!string.IsNullOrWhiteSpace(_settings.FileFormat))
					extension = Path.GetExtension(_settings.FileFormat).Replace(".", "");
				if (string.IsNullOrWhiteSpace(extension))
					extension = "html";
				var folder = _settings.DestinationFolder;
				if (string.IsNullOrWhiteSpace(folder))
				{
					folder = _settings.ProjectName;
					if (string.IsNullOrWhiteSpace(folder))
						folder = GetDataFolder(identity);
				}
				if (!Directory.Exists(folder))
					Directory.CreateDirectory(folder);
				var request = resultItems[0].Request;
				var pageIndex = request.GetProperty(ProjectDefinition.PageIndexKey);
				var pageDepth = request.GetProperty(Page.Depth);
				var file = Path.Combine(folder, $"{pageIndex ?? 1}_{pageDepth}_{request.GetIdentity()}.{extension}");

				using (var writer = new StreamWriter(File.OpenWrite(file), Encoding.UTF8))
				{
					try
					{
						//await writer.WriteLineAsync("Page: " + context.Response.Request.PageIndex.ToString());
						foreach (var item in resultItems)
						{
							var content = item["Content"];

							writer.WriteLine(content);
							//await Writer.WriteLineAsync(items.ToString());
						}
					}
					finally
					{
						//Writer.Close();
						//Writer.Dispose();
					}
				}

				// Storage data to DB. 可以自由实现插入数据库或保存到文件
			}
		}

		private class FileStoragePipeline : BaseFilePipeline
		{
			private ProjectDefinition _settings;
			private IFileStorageService _fileService;
			public FileStoragePipeline(IFileStorageService fileService, ProjectDefinition settings) : base(null)
			{
				this._settings = settings;
				this._fileService = fileService;
			}
			public override void Process(IList<ResultItems> resultItems, dynamic sender = null)
			{
				if (resultItems != null && resultItems.Count > 0)
				{
					var request = resultItems[0].Request;
					request.AddCountOfResults(1);
					request.AddEffectedRows(1);
					bool saved = _fileService.SaveAsync(_settings.DestinationFolder, request.GetFileName(_settings.FileFormat), resultItems[0]["PageSource"].ToString()).Result;
				}

				//var pageIndex = request.GetProperty(ProjectDefinition.PageIndexKey);
				//var pageDepth = request.GetProperty(Page.Depth);
				//var file = Path.Combine(folder, $"{pageIndex ?? 1}_{pageDepth}_{request.GetIdentity()}.{extension}");
			}
		}

		private class SimplePageProcessor : BasePageProcessor
		{
			private ProjectDefinition _settings;
			public SimplePageProcessor(ProjectDefinition settings) : base()
			{
				this._settings = settings;
			}
			protected override void Handle(Page page)
			{
				var mapping = _settings.Mapping;
				var currentDepthValue = page.Request.GetProperty(Page.Depth);
				var currentDepth = 1;
				if (currentDepthValue != null)
					currentDepth = Convert.ToInt32(currentDepthValue);
				if (mapping != null)
				{
					bool filteredOut = false;
					if (mapping.OnlyPagesAtDepth.GetValueOrDefault() >= 1)
					{
						if (currentDepth != mapping.OnlyPagesAtDepth.Value)
						{
							filteredOut = true;
						}
					}
					if (!filteredOut)
					{
						if (!string.IsNullOrWhiteSpace(mapping.ItemCssSelector))
						{
							var items = new List<dynamic>();
							var itemNodes = page.Selectable().XPath(mapping.ItemCssSelector).Nodes();
							foreach (var note in itemNodes)
							{
								var item = new Dictionary<string, string>();
								foreach (var field in mapping.Mapping)
								{
									item.Add(field.Field, note.XPath(field.CssSelector).GetValue());
								}
								if (item.Count > 0)
								{
									item.Add("PageSourceURL", page.Request.Url);
									items.Add(item);

								}
							}
							if (items.Count > 0)
							{
								page.AddResultItem("Content", JsonConvert.SerializeObject(items, Formatting.Indented));
							}
							//else
							//{
							//	page.AddResultItem("PageSource", page.Content);
							//}

						}
						else
						{
							if (mapping.Mapping != null && mapping.Mapping.Length > 0)
							{
								var item = new Dictionary<string, string>();
								foreach (var field in mapping.Mapping)
								{
									var value = page.Selectable().XPath(field.CssSelector).GetValue();
									if (value != null)
										value = value.Replace("\t", "").Trim();
									item.Add(field.Field, value);
								}
								if (item.Count > 0)
								{
									item.Add("PageSourceURL", page.Request.Url);
									page.AddResultItem("Content", JsonConvert.SerializeObject(item, Formatting.Indented));
								}
							}
							else
							{
								//var item = new Dictionary<string, string>();
								//item.Add("PageSourceURL", page.Request.Url);
								//item.Add("Content", page.Content.ToString());
								page.AddResultItem("PageSource", page.Content);
							}
						}
					}
				}

				if (!string.IsNullOrWhiteSpace(_settings.ItemUrlsSelector))
				{
					var selectors = _settings.ItemUrlsSelector.Split(";".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
					//new string[] { "//article/h1[@class='title_news']/a[1]", "//article[@class='list_news']/h4[@class='title_news']/a[1]" }
					if (selectors != null && selectors.Length > 0)
					{
						foreach (var selector in selectors)
						{
							// Add target requests to scheduler. 解析需要采集的URL
							foreach (var url in page.Selectable().SelectList(Selectors.XPath(selector)).Links().Nodes())
							{
								var newRequest = new Request(url.GetValue());
								newRequest.AddProperty(Page.Depth, page.Request.GetProperty(Page.Depth));
								// assign PageIndex to the same Page with the current request.
								newRequest.AddProperty(ProjectDefinition.PageIndexKey, page.Request.GetProperty(ProjectDefinition.PageIndexKey));
								page.AddTargetRequest(newRequest, true);
							}
						}
					}
				}


				if (!string.IsNullOrWhiteSpace(_settings.NextPageSelector))
				{
					int pageIndex = 1;

					var pageIndexProp = page.Request.GetProperty(ProjectDefinition.PageIndexKey);
					if (pageIndexProp != null)
						pageIndex = Convert.ToInt32(pageIndexProp);
					// only find Next Page Url if not exceed Page Limit.
					if (_settings.PageLimit.GetValueOrDefault() == 0 || pageIndex < _settings.PageLimit.GetValueOrDefault())
					{
						// Add target requests to scheduler. 解析需要采集的URL
						//foreach (var url in page.Selectable().SelectList(Selectors.XPath(_settings.NextPageSelector)).Links().Nodes())
						var url = page.Selectable().SelectList(Selectors.XPath(_settings.NextPageSelector)).Links().GetValue();
						if (!string.IsNullOrWhiteSpace(url))
						{
							// Increase PageIndex
							var nextRequest = new Request(url);
							nextRequest.AddProperty(Page.Depth, page.Request.GetProperty(Page.Depth));
							nextRequest.AddProperty(ProjectDefinition.PageIndexKey, pageIndex + 1);
							page.AddTargetRequest(nextRequest, false);
						}
					}
					else
					{

					}
				}

			}
		}

		private class YoukuVideo
		{
			public string Name { get; set; }
		}
	}
}

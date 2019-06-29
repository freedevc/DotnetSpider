using DotnetSpider.Data;
using DotnetSpider.Data.Parser;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DotnetSpider.Sample.Parsers
{
	class SimpleItemDataParser : DataParser
	{
		private ItemMapping _mapping;
		public SimpleItemDataParser(string itemFollowSelectors, ItemMapping mapping)
		{
			//	CanParse = DataParserHelper.CanParseByRegex("vnexpress\\.com");
			if (!string.IsNullOrWhiteSpace(itemFollowSelectors))
			{
				var selectors = itemFollowSelectors.Split(';', StringSplitOptions.RemoveEmptyEntries);
				//new string[] { "//article/h1[@class='title_news']/a[1]", "//article[@class='list_news']/h4[@class='title_news']/a[1]" }
				if (selectors != null && selectors.Length > 0)
					QueryFollowRequests = DataParserHelper.QueryFollowRequestsByXPath(selectors);
			}
			_mapping = mapping;
		}

		protected override Task<DataFlowResult> Parse(DataFlowContext context)
		{

			if (_mapping != null)
			{
				if (_mapping.Deepth.GetValueOrDefault() >= 1)
				{
					if (context.Response.Request.Depth != _mapping.Deepth.Value)
					{
						context.ClearItems();
						return Task.FromResult(DataFlowResult.Success);
					}
				}
				if (!string.IsNullOrWhiteSpace(_mapping.ItemCssSelector))
				{
					var items = new List<dynamic>();
					var itemNodes = context.GetSelectable().XPath(_mapping.ItemCssSelector).Nodes();
					foreach (var note in itemNodes)
					{
						var item = new Dictionary<string, string>();
						foreach (var field in _mapping.Mapping)
						{
							item.Add(field.Field, note.XPath(field.CssSelector).GetValue());
						}
						if (item.Count > 0)
						{
							item.Add("PageSourceURL", context.Response.Request.Url);
							items.Add(item);
						}
					}
					if (items.Count > 0)
					{
						context.AddItem("Content", JsonConvert.SerializeObject(items));
					}
				}
				else
				{
					if (_mapping.Mapping != null && _mapping.Mapping.Length > 0)
					{
						var item = new Dictionary<string, string>();
						foreach (var field in _mapping.Mapping)
						{
							var value =  context.GetSelectable().XPath(field.CssSelector).GetValue();
							if (value != null)
								value = value.Replace("\t","").Trim();
							item.Add(field.Field, value);
						}
						if (item.Count > 0)
						{
							item.Add("PageSourceURL", context.Response.Request.Url);
							context.AddItem("Content", JsonConvert.SerializeObject(item, Formatting.Indented));
						}
					}
					else
					{
						context.AddItem("PageSourceURL", context.Response.Request.Url);
						context.AddItem("Content", context.Response.RawText);
					}
				}

			}
			//var item = context.GetSelectable().XPath("//h1[@class='title_news_detail mb10']").GetValue();
			//var item = context.GetSelectable().XPath("//h1[@class='title_news_detail mb10']").GetValue();
			//if (!string.IsNullOrWhiteSpace(item))
			//{
			//	//	context.AddItem("Vnexpress", item);
			//	context.AddItem("Content:", context.Response.RawText);
			//}
			//else
			//	context.ClearItems();
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
	class SimplePaginationDataParser : DataParser
	{
		private string _nextPageSelector;
		public SimplePaginationDataParser(string nextPageSelector)
		{
			_nextPageSelector = nextPageSelector;
			//	CanParse = DataParserHelper.CanParseByRegex("cnblogs\\.com");
			//	QueryFollowRequests = DataParserHelper.QueryFollowRequestsByXPath(new string[] { "//h4[@class='title_news']/a", "//p[@id='pagination']/a[@class='next']" });
		}

		protected override Task<DataFlowResult> Parse(DataFlowContext context)
		{
			var nextPageUrl = context.GetSelectable().XPath(_nextPageSelector).Links().GetValue();
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
}

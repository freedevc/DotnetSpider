using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DotnetSpider.Downloader;
using DotnetSpider.Selector;
using Microsoft.Extensions.Logging;

namespace DotnetSpider.Data.Parser
{
	/// <summary>
	/// 数据解析器
	/// </summary>
	public abstract class DataParserBase : DataFlowBase
	{
		/// <summary>
		/// Determine if the current request can be parsed
		/// </summary>
		public Func<Request, bool> CanParse { get; set; }

		/// <summary>
		/// Query the next request of the current request
		/// </summary>
		public Func<DataFlowContext, List<string>> QueryFollowRequests { get; set; }

		/// <summary>
		/// Selector generation method
		/// </summary>
		public Func<DataFlowContext, ISelectable> SelectableFactory { get; set; }

		/// <summary>
		/// Data analysis
		/// </summary>
		/// <param name="context">Processing context</param>
		/// <returns></returns>
		public override async Task<DataFlowResult> HandleAsync(DataFlowContext context)
		{
			if (context?.Response == null)
			{
				Logger?.LogError("Data context or response content is empty");
				return DataFlowResult.Failed;
			}

			try
			{
				// Skip if not matched, does not affect the execution of other data stream processors
				if (CanParse != null && !CanParse(context.Response.Request))
				{
					return DataFlowResult.Success;
				}
				// [Doanh]: call this first to initialize _selectable Instance for the context.
				SelectableFactory?.Invoke(context);

				var parserResult = await Parse(context);

				var urls = QueryFollowRequests?.Invoke(context);
				AddTargetRequests(context, urls);

				if (parserResult == DataFlowResult.Failed || parserResult == DataFlowResult.Terminated)
				{
					return parserResult;
				}

				return DataFlowResult.Success;
			}
			catch (Exception e)
			{
				Logger?.LogError($"任务 {context.Response.Request.OwnerId} 数据解析发生异常: {e}");
				return DataFlowResult.Failed;
			}
		}

		protected virtual void AddTargetRequests(DataFlowContext dfc, List<string> urls)
		{
			if (urls != null && urls.Count > 0)
			{
				var followRequests = new List<Request>();
				foreach (var url in urls)
				{
					var followRequest = CreateFromRequest(dfc.Response.Request, url);
					if (CanParse == null || CanParse(followRequest))
					{
						followRequests.Add(followRequest);
					}
				}

				dfc.FollowRequests.AddRange(followRequests.ToArray());
			}
		}

		protected virtual Request CreateFromRequest(DataFlowContext dfc, string url)
		{
			return CreateFromRequest(dfc.Response.Request, url);
		}

		protected virtual IEnumerable<Request> CreateFromRequests(DataFlowContext dfc, IEnumerable<string> urls)
		{
			return CreateFromRequests(dfc.Response.Request, urls);
		}

		/// <summary>
		/// Create the next level request for the current request
		/// </summary>
		/// <param name="current">Current request</param>
		/// <param name="url">Next level request</param>
		/// <returns></returns>
		protected virtual Request CreateFromRequest(Request current, string url)
		{
			// TODO: 确认需要复制哪些字段
			// TODO: Confirm which fields need to be copied
			var request = new Request(url, current.GetProperties())
			{
				Url = url,
				Depth = current.Depth,
				Body = current.Body,
				Method = current.Method,
				AgentId = current.AgentId,
				RetriedTimes = 0,
				OwnerId = current.OwnerId,
				PageIndex = current.PageIndex
			};
			return request;
		}

		protected virtual IEnumerable<Request> CreateFromRequests(Request current, IEnumerable<string> urls)
		{
			var list = new List<Request>();
			foreach (var url in urls)
			{
				list.Add(CreateFromRequest(current, url));
			}

			return list;
		}

		/// <summary>
		/// Parses 
		/// </summary>
		/// <param name="context">DataFlow Context</param>
		/// <returns></returns>
		protected abstract Task<DataFlowResult> Parse(DataFlowContext context);
	}
}
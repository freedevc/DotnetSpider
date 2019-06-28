using System.IO;
using System.Text;
using System.Threading.Tasks;
using DotnetSpider.Core;
using DotnetSpider.Data;
using DotnetSpider.Data.Storage;
using Newtonsoft.Json;
namespace DotnetSpider.Sample.samples.HtmlFileStorage
{
	class HtmlFileStorage: FileStorageBase
	{
		/// <summary>
		/// 根据配置返回存储器
		/// </summary>
		/// <param name="options">配置</param>
		/// <returns></returns>
		public static HtmlFileStorage CreateFromOptions(ISpiderOptions options)
		{
			return new HtmlFileStorage();
		}

		protected override async Task<DataFlowResult> Store(DataFlowContext context)
		{
			//var file = Path.Combine(GetDataFolder(context.Response.Request.OwnerId), $"{context.Response.Request.Hash}.html");
			var file = Path.Combine(GetDataFolder(context["ProjectName"]?? context.Response.Request.OwnerId), $"{context.Response.Request.PageIndex}_{context.Response.Request.Hash}.html");

			using (var writer = new StreamWriter(File.OpenWrite(file), Encoding.UTF8))
			{
				try
				{
					var items = context.GetItems();
					//await writer.WriteLineAsync("Page: " + context.Response.Request.PageIndex.ToString());
					foreach (var item in items)
					{
						await writer.WriteLineAsync(item.Value);
						//await Writer.WriteLineAsync(items.ToString());
					}
				}
				finally
				{
					//Writer.Close();
					//Writer.Dispose();
				}
			}
			return DataFlowResult.Success;
		}
	}
}
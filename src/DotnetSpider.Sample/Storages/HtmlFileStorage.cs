using System.IO;
using System.Text;
using System.Threading.Tasks;
using DotnetSpider.Core;
using DotnetSpider.Data;
using DotnetSpider.Data.Storage;
using Newtonsoft.Json;
namespace DotnetSpider.Sample
{
	class HtmlFileStorage: FileStorageBase
	{
		/// <summary>
		/// 根据配置返回存储器
		/// </summary>
		/// <param name="options">配置</param>
		/// <returns></returns>
		public static HtmlFileStorage CreateFromOptions(ProjectDefinition options)
		{
			return new HtmlFileStorage(options);
		}
		private ProjectDefinition _definition;
		public HtmlFileStorage(ProjectDefinition definition)
		{
			_definition = definition;
		}
		protected override async Task<DataFlowResult> Store(DataFlowContext context)
		{
			//var file = Path.Combine(GetDataFolder(context.Response.Request.OwnerId), $"{context.Response.Request.Hash}.html");
			var extension = "html";
			if (!string.IsNullOrWhiteSpace(_definition.FileFormat))
				extension = Path.GetExtension(_definition.FileFormat).Replace(".", "");
			if (string.IsNullOrWhiteSpace(extension))
				extension = "html";
			var folder = _definition.FileStorage;
			if (string.IsNullOrWhiteSpace(folder))
			{
				folder = _definition.ProjectName;
				if (string.IsNullOrWhiteSpace(folder))
					folder = context.Response.Request.OwnerId;
				folder = GetDataFolder(folder);
			}
			if (!Directory.Exists(folder))
				Directory.CreateDirectory(folder);
			var file = Path.Combine(folder, $"{context.Response.Request.PageIndex}_{context.Response.Request.Depth}_{context.Response.Request.Hash}.{extension}");

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
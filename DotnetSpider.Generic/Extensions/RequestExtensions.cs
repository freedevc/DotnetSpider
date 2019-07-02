using DotnetSpider.Downloader;
using DotnetSpider.Core.Infrastructure;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DotnetSpider.Generic.Extensions
{
	public static class RequestExtensions
	{
		public static string GetFileName(this Request request, string fileExtenstion)
		{
			var file = request.Url.ToLower();
			var extension = Path.GetExtension(fileExtenstion).Replace(".", "").Trim();
			file = file.ToMd5() + "." + extension;
			return file;
		}
	}
}

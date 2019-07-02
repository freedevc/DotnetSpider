using DotnetSpider.Proxy;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace DotnetSpider.Generic.Proxy
{

	public class StringProxySupplier : IProxySupplier
	{
		private readonly string[] _urls;

		//private readonly HttpClient _client = new HttpClient(new HttpClientHandler
		//{
		//	AllowAutoRedirect = true,
		//	AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip,
		//	UseProxy = true,
		//	UseCookies = true,
		//	MaxAutomaticRedirections = 10
		//});

		public StringProxySupplier(string[] urls)
		{
			_urls = urls;
		}

		public Dictionary<string, ProxyInfo> GetProxies()
		{
			var dict = new Dictionary<string, ProxyInfo>();
			try
			{
				if (_urls == null || _urls.Length == 0) return dict;
				//var rows = _client.GetStringAsync(_url).Result;
				var proxies = _urls;// rows.Split(new[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);

				foreach (var proxy in proxies)
				{
					if (!dict.ContainsKey(proxy))
					{
						dict.Add(proxy, new ProxyInfo(new WebProxy(proxy, false)));
					}
				}
			}
			catch { }
			return dict;
		}
		//public Dictionary<string, ProxyInfo> GetProxies()
		//{
		//	var dict = new Dictionary<string, ProxyInfo>();
		//	try
		//	{
		//		var rows = _client.GetStringAsync(_url).Result;
		//		var proxies = rows.Split(new[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);

		//		foreach (var proxy in proxies)
		//		{
		//			if (!dict.ContainsKey(proxy))
		//			{
		//				dict.Add(proxy, new ProxyInfo(new WebProxy(proxy, false)));
		//			}
		//		}
		//	}
		//	catch { }
		//	return dict;
		//}
	}
}

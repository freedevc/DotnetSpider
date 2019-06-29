using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using DotnetSpider.Core;

namespace DotnetSpider.Downloader
{
	/// <summary>
	/// 验证代理是否正常
	/// </summary>
	public class FakeProxyValidator : IProxyValidator
	{

		public FakeProxyValidator()
		{
		}

		public bool IsAvailable(WebProxy proxy)
		{
			return true;
		}

		/// <summary>
		/// 代理验证器
		/// 提供代理的验证
		/// </summary>
		
	}
}
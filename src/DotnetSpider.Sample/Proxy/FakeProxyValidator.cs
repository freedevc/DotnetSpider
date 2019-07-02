using DotnetSpider.Proxy;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace DotnetSpider.Sample.Proxy
{
	public class FakeProxyValidator : IProxyValidator
	{
		private bool _skipValidation;
		public FakeProxyValidator(bool skipValidation = true)
		{
			this._skipValidation = skipValidation;
		}
	
		public bool IsAvailable(WebProxy proxy)
		{
			try
			{
				if(_skipValidation)
					return true;
				TcpClient client = new TcpClient(proxy.Address.Host, proxy.Address.Port);
				return true;
			}
			catch //(Exception ex1)
			{
				return false;
			}
		}

		/// <summary>
		/// 代理验证器
		/// 提供代理的验证
		/// </summary>

	}
}

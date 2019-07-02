using DotnetSpider.Core.Infrastructure;
using DotnetSpider.Proxy;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;

namespace DotnetSpider.Sample.Proxy
{
	public class CustomHttpProxyPool : IHttpProxyPool
	{
		private readonly IProxySupplier _supplier;
		private readonly List<ProxyInfo> _proxyQueue = new List<ProxyInfo>();
		private readonly Dictionary<string, ProxyInfo> _proxies = new Dictionary<string, ProxyInfo>();
		private bool _isDispose;
		private readonly int _reuseInterval;
		private readonly object _proxyQueueLocker = new object();

		public static IHttpProxyPool Instance;

		public IProxyValidator ProxyValidator { get; set; } = new DefaultProxyValidator();

		/// <summary>
		/// 构造方法
		/// </summary>
		/// <param name="supplier">代理提供接口</param>
		/// <param name="reuseInterval">代理不被再次使用的间隔</param>
		public CustomHttpProxyPool(IProxySupplier supplier, int reuseInterval = 500)
		{
			_supplier = supplier ?? throw new ProxyException($"{nameof(supplier)} is null.");

			_reuseInterval = reuseInterval;
			if (ProxyValidator != null)
			{
				ThreadPool.QueueUserWorkItem(RefreshProxies);
			}
		}

		/// <summary>
		/// 从代理池中取一个代理
		/// </summary>
		/// <returns>代理</returns>
		public WebProxy GetProxy()
		{
			for (int i = 0; i < 3600; ++i)
			{
				lock (_proxyQueueLocker)
				{
					var proxy = GetFirstAvaliableProxy();
					if (proxy != null)
					{
						proxy.SetLastBorrowTime(DotnetSpider.Proxy.DateTimeUtil.GetCurrentUnixTimeNumber());
					//	_proxyQueue.Remove(proxy);
						return proxy.GetWebProxy();
					}
				}
				Thread.Sleep(1000);
			}

			throw new ProxyException("There is no available proxy.");
		}

		/// <summary>
		/// 把代理返回给代理池
		/// </summary>
		/// <param name="proxy">代理</param>
		/// <param name="statusCode">通过此代理请求数据后的返回状态</param>
		public void ReturnProxy(WebProxy proxy, HttpStatusCode statusCode)
		{
			if (proxy == null)
			{
				return;
			}
			var key = $"{proxy.Address.Host}:{proxy.Address.Port}";
			if (!_proxies.ContainsKey(key))
			{
				return;
			}
			ProxyInfo p = _proxies[key];
			switch (statusCode)
			{
				case HttpStatusCode.OK:
					p.SetFailedNum(0);
					p.SetReuseTime(_reuseInterval);
					p.RecordResponse();
					break;
				case HttpStatusCode.Forbidden:
					p.Fail();
					p.SetReuseTime(_reuseInterval * p.FailedNum);
					break;
				case HttpStatusCode.NotFound:
					p.Fail();
					p.SetReuseTime(_reuseInterval * p.FailedNum);
					break;
				default:
					p.Fail();
					p.SetReuseTime(_reuseInterval * p.FailedNum);
					break;
			}
			if (p.FailedNum > 20)
			{
				return;
			}
			if (ProxyValidator != null && p.FailedNum % 3 == 0 && ProxyValidator.IsAvailable(proxy))
			{
				return;
			}
			//lock (_proxyQueueLocker)
			//{
			//	_proxyQueue.Add(p);
			//}
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose()
		{
			_isDispose = true;
		}

		private ProxyInfo GetFirstAvaliableProxy()
		{
			var currentUnixTimeNumber = DotnetSpider.Proxy.DateTimeUtil.GetCurrentUnixTimeNumber();
			foreach (var proxy in _proxyQueue)
			{
				if (currentUnixTimeNumber - proxy.GetLastUseTime() > _reuseInterval)
				{
					return proxy;
				}
			}
			return null;
		}

		private void RefreshProxies(object threadState)
		{
			if (ProxyValidator == null || _supplier == null)
			{
				return;
			}
			//ThreadCommonPool threadCommonPool = new ThreadCommonPool(4);
			while (!_isDispose)
			{
				if (_proxyQueue.Count < 50)
				{
					var proxies = _supplier.GetProxies();

					foreach (var proxy in proxies)
					{
						//threadCommonPool.QueueUserWork(item =>
						//{
						lock (_proxyQueueLocker)
						{
							if (!_proxies.ContainsKey(proxy.Key))
							{
								if (ProxyValidator.IsAvailable(proxy.Value.WebProxy))
								{
									proxy.Value.SetFailedNum(0);
									proxy.Value.SetReuseTime(_reuseInterval);

									lock (_proxyQueueLocker)
									{
										_proxyQueue.Add(proxy.Value);
									}
									_proxies.Add(proxy.Key, proxy.Value);
								}
							}
						}
						//}, proxy);
					}
				}
				Thread.Sleep(30000);
			}
		}
	}
}

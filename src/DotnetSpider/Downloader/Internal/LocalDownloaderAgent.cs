using DotnetSpider.MessageQueue;
using DotnetSpider.Network;
using Microsoft.Extensions.Logging;

namespace DotnetSpider.Downloader.Internal
{
	/// <summary>
	/// 本地下器代理
	/// </summary>
	public class LocalDownloaderAgent : DefaultDownloaderAgent
	{
		/// <summary>
		/// 构造方法
		/// </summary>
		/// <param name="options">下载器代理选项</param>
		/// <param name="mq">消息队列</param>
		/// <param name="downloaderAllocator">分配下载器的接口</param>
		/// <param name="networkCenter">网络中心</param>
		/// <param name="logger">日志接口</param>
		public LocalDownloaderAgent(IDownloaderAgentOptions options,IDynamicMessageQueue dmq,
			IMessageQueue mq, IDownloaderAllocator downloaderAllocator, NetworkCenter networkCenter,
			ILogger<LocalDownloaderAgent> logger) : base(options,dmq,
			mq, downloaderAllocator, networkCenter, logger)
		{
			// ConfigureDownloader = downloader => downloader.Logger = null;
		}
	}
}
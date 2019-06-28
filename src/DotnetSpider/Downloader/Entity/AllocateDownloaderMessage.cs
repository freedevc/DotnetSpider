using System;
using DotnetSpider.Core;

namespace DotnetSpider.Downloader.Entity
{
	/// <summary>
	/// Options for assigning the downloader agent (message)
	/// 分配下载器代理的选项(消息)
	/// </summary>
	public class AllocateDownloaderMessage
	{
		/// <summary>
		/// Download Policy
		/// </summary>
		public DownloadPolicy DownloadPolicy { get; set; }

		/// <summary>
		/// 任务标识
		/// </summary>
		public string OwnerId { get; set; }

		/// <summary>
		/// 下载器类别
		/// </summary>
		public DownloaderType Type { get; set; }

		/// <summary>
		/// Cookie
		/// </summary>
		public Cookie[] Cookies { get; set; }

		/// <summary>
		/// 是否使用代理
		/// </summary>
		public bool UseProxy { get; set; }

		/// <summary>
		/// 是否使用 Cookie
		/// </summary>
		public bool UseCookies { get; set; }

		/// <summary>
		/// 是否自动跳转
		/// </summary>
		public bool AllowAutoRedirect { get; set; }

		/// <summary>
		/// 下载超时
		/// </summary>
		public int Timeout { get; set; }

		/// <summary>
		/// 是否进行 HTML 转码
		/// </summary>
		public bool DecodeHtml { get; set; }

		/// <summary>
		/// The number of downloader agents that need to be allocated
		/// 需要分配的下载器代理的个数
		/// </summary>
		public int DownloaderCount { get; set; }

		/// <summary>
		/// Download retries
		/// 下载重试次数
		/// </summary>
		public int RetryTimes { get; set; } = 3;

		/// <summary>
		/// 消息创建时间
		/// </summary>
		public DateTime CreationTime { get; set; }
	}
}
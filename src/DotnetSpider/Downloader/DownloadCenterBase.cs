using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DotnetSpider.Core;
using DotnetSpider.Downloader.Entity;
using DotnetSpider.MessageQueue;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace DotnetSpider.Downloader
{
	/// <summary>
	/// 下载中心
	/// </summary>
	public abstract class DownloadCenterBase : IDownloadCenter
	{
		private bool _isRunning;

		protected readonly ConcurrentDictionary<string, DownloaderAgent> Agents =
			new ConcurrentDictionary<string, DownloaderAgent>();

		protected readonly ConcurrentDictionary<string, Tuple<AllocateDownloaderMessage, string[]>> AllocatedAgents
			= new ConcurrentDictionary<string, Tuple<AllocateDownloaderMessage, string[]>>();

		/// <summary>
		/// 消息队列
		/// </summary>
		protected readonly IMessageQueue Mq;

		protected readonly IDynamicMessageQueue Dmq;
		/// <summary>
		/// 系统选项
		/// </summary>
		protected readonly ISpiderOptions Options;

		/// <summary>
		/// 日志接口
		/// </summary>
		protected readonly ILogger Logger;

		/// <summary>
		/// 下载器代理存储
		/// </summary>
		protected readonly IDownloaderAgentStore DownloaderAgentStore;

		/// <summary>
		/// 构造方法
		/// </summary>
		/// <param name="mq">消息队列</param>
		/// <param name="downloaderAgentStore">下载器代理存储</param>
		/// <param name="options">系统选项</param>
		/// <param name="logger">日志接口</param>
		protected DownloadCenterBase(
			IDynamicMessageQueue dmq,
			IMessageQueue mq,
			IDownloaderAgentStore downloaderAgentStore,
			ISpiderOptions options,
			ILogger logger)
		{
			Dmq = dmq;
			Mq = mq;
			DownloaderAgentStore = downloaderAgentStore;
			Logger = logger;
			Options = options;
		}

		/// <summary>
		/// 启动下载中心
		/// </summary>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		/// <exception cref="SpiderException"></exception>
		public async Task StartAsync(CancellationToken cancellationToken)
		{
			if (_isRunning)
			{
				throw new SpiderException("下载中心正在运行中");
			}

			await DownloaderAgentStore.EnsureDatabaseAndTableCreatedAsync();

			await SyncDownloaderAgentData();

			StartSyncDownloaderAgentDataService().ConfigureAwait(false).GetAwaiter();

			Mq.Subscribe(Framework.DownloaderCenterTopic, async message =>
			{
				var commandMessage = message.ToCommandMessage();
				if (commandMessage == null)
				{
					Logger.LogWarning($"接收到非法消息: {message}");
					return;
				}

				switch (commandMessage.Command)
				{
					case Framework.RegisterCommand:
						{
							// 此处不考虑消息的超时，一是因为节点数量不会很多，二是因为超时的可以释放掉
							// The timeout of the message is not considered here, because the number of nodes is not much, and the second is because the timeout can be released.
							var agent = JsonConvert.DeserializeObject<DownloaderAgent>(commandMessage.Message);
							if (agent != null)
							{
								Agents.AddOrUpdate(agent.Id, x => agent, (s, a) =>
								{
									a.LastModificationTime = agent.LastModificationTime;
									return a;
								});
								await DownloaderAgentStore.RegisterAsync(agent);
								Logger.LogInformation($"Register the Downloader Agent {agent.Id} successfully");
							}
							else
							{
								Logger.LogError($"Register Downloader Agent Message Is Incorrect: {commandMessage.Message}");
							}

							break;
						}
					case Framework.HeartbeatCommand:
						{
							var heartbeat = JsonConvert.DeserializeObject<DownloaderAgentHeartbeat>(commandMessage.Message);
							if (heartbeat != null)
							{
								if ((DateTime.Now - heartbeat.CreationTime).TotalSeconds < Options.MessageExpiredTime)
								{
									if (Agents.ContainsKey(heartbeat.AgentId))
									{
										Agents[heartbeat.AgentId].RefreshLastModificationTime();
									}

									await DownloaderAgentStore.HeartbeatAsync(heartbeat);
									Logger.LogDebug($"Downloader Agent {heartbeat.AgentId} updates heartbeat success");
								}
								else
								{
									Logger.LogWarning($"Downloader Agent {heartbeat.AgentId} update Heartbeat Period");
								}
							}
							else
							{
								Logger.LogError($"Downloader Agent heartbeat information is incorrect: {commandMessage.Message}");
							}

							break;
						}
					case Framework.AllocateDownloaderCommand:
						{
							var options = JsonConvert.DeserializeObject<AllocateDownloaderMessage>(commandMessage.Message);
							if (options != null)
							{
								if ((DateTime.Now - options.CreationTime).TotalSeconds < Options.MessageExpiredTime)
								{
									await AllocateAsync(options, commandMessage.Message);
								}
								else
								{
									Logger.LogWarning($"任务 {options.OwnerId} 分配下载代理器过期");
								}
							}
							else
							{
								Logger.LogError($"分配下载代理器过期信息不正确: {commandMessage.Message}");
							}

							break;
						}
					case Framework.DownloadCommand:
						{
							var requests = JsonConvert.DeserializeObject<Request[]>(commandMessage.Message);
							if (requests != null)
							{
								requests = requests.Where(x =>
									(DateTime.Now - x.CreationTime).TotalSeconds <= Options.MessageExpiredTime).ToArray();
								if (requests.Length > 0)
								{
									var ownerId = requests[0].OwnerId;
									foreach (var request in requests)
									{
										request.CreationTime = DateTime.Now;
									}

									await EnqueueRequests(ownerId, requests);
									Logger.LogDebug($"任务 {ownerId} 下载请求分发成功");
								}
							}
							else
							{
								Logger.LogError($"任务请求信息不正确: {commandMessage.Message}");
							}

							break;
						}
				}
			});
			if (Dmq != null)
			{
				Dmq.Subscribe(Framework.DownloaderCenterTopic, async (cmd, message) =>
				{
				//var commandMessage = message.ToCommandMessage();
				if (message == null)
					{
						Logger.LogWarning($"接收到非法消息: {cmd}");
						return;
					}

					switch (cmd)
					{

						case Framework.DownloadCommand:
							{
								var requests = message as Request[];// JsonConvert.DeserializeObject<Request[]>(commandMessage.Message);
							if (requests == null)
								{
									var request = message as Request;
									if (request != null)
										requests = new Request[] { request };
								}
								if (requests != null)
								{
									requests = requests.Where(x =>
										(DateTime.Now - x.CreationTime).TotalSeconds <= Options.MessageExpiredTime).ToArray();
									if (requests.Length > 0)
									{
										var ownerId = requests[0].OwnerId;
										foreach (var request in requests)
										{
											request.CreationTime = DateTime.Now;
										}

										await EnqueueRequests(ownerId, requests);
										Logger.LogDebug($"任务 {ownerId} 下载请求分发成功");
									}
								}
								else
								{
									Logger.LogError($"任务请求信息不正确: message");
								}

								break;
							}
					}
				});
			}
			Logger.LogInformation("下载中心启动完毕");
		}

		public Task StopAsync(CancellationToken cancellationToken)
		{
			Mq.Unsubscribe(Framework.DownloaderCenterTopic);
			if(Dmq != null)
				Dmq.Unsubscribe(Framework.DownloaderCenterTopic);
			_isRunning = false;
			Logger.LogInformation("下载中心退出");
#if NETFRAMEWORK
			return Framework.CompletedTask;
#else
			return Task.CompletedTask;
#endif
		}


		/// <summary>
		/// 分配下载器代理
		/// </summary>
		/// <param name="allotDownloaderMessage">分配下载器代理的消息</param>
		/// <param name="message">分配下载器代理的消息</param>
		/// <returns></returns>
		protected virtual async Task<bool> AllocateAsync(AllocateDownloaderMessage allotDownloaderMessage,
			string message)
		{
			var agents = Agents.Values;

			// Calculate the number of allocations required
			var count = allotDownloaderMessage.DownloaderCount >= agents.Count
				? agents.Count
				: allotDownloaderMessage.DownloaderCount;

			var agentIds = agents.OrderBy(_ => Guid.NewGuid()).Take(count).Select(x => x.Id).ToArray();

			// Agents are updated asynchronously, so the final allocation result is used to determine whether the allocation is correct.
			if (agentIds.Length == 0)
			{
				Logger.LogInformation($"任务 {allotDownloaderMessage.OwnerId} 未分配到下载器代理");
				return false;
			}

			if (agentIds.Length < allotDownloaderMessage.DownloaderCount)
			{
				Logger.LogWarning($"任务 {allotDownloaderMessage.OwnerId} 未足额分配下载器代理");
			}

			// Send a message to let the download agent allocate the downloader
			var msg =
				$"|{Framework.AllocateDownloaderCommand}|{JsonConvert.SerializeObject(allotDownloaderMessage)}";
			foreach (var agent in agents)
			{
				await Mq.PublishAsync(agent.Id, msg);
			}

			// Save node allocation information to the storage
			await DownloaderAgentStore.AllocateAsync(allotDownloaderMessage.OwnerId, message, agentIds);
			// Update allocation information in the cache
			AllocatedAgents.AddOrUpdate(allotDownloaderMessage.OwnerId,
				new Tuple<AllocateDownloaderMessage, string[]>(allotDownloaderMessage, agentIds), (s, tuple) => tuple);
			Logger.LogInformation(
				$"任务 {allotDownloaderMessage.OwnerId} 分配下载代理器成功: {JsonConvert.SerializeObject(agentIds)}");
			return true;
		}

		/// <summary>
		/// Push download request to the downloader queue
		/// </summary>
		/// <param name="ownerId">任务标识</param>
		/// <param name="requests">请求</param>
		/// <returns></returns>
		protected virtual async Task EnqueueRequests(string ownerId, Request[] requests)
		{
			if (requests == null || requests.Length == 0)
			{
				return;
			}

			// If it is not in the cache, it is taken from the database. This scenario only occurs when the download center switches
			AllocatedAgents.AddOrUpdate(ownerId, new Tuple<AllocateDownloaderMessage, string[]>(
				JsonConvert.DeserializeObject<AllocateDownloaderMessage>(
					await DownloaderAgentStore.GetAllocateDownloaderMessageAsync(ownerId)),
				(await DownloaderAgentStore.GetAllocatedListAsync(ownerId)).Select(x => x.AgentId).ToArray()
			), (s, tuple) => tuple);

			var allocateDownloaderMessage = AllocatedAgents[ownerId].Item1;
			var agentIds = AllocatedAgents[ownerId].Item2;
			if (agentIds.Length <= 0)
			{
				Logger.LogError($"任务 {ownerId} 未分配到下载器代理");
			}

			var onlineAgents = new List<DownloaderAgent>();
			// 判断分配的下载器代理是否活跃，因为在下载中心发生切换时需要靠数据库同步数据，则同步了 2 次(15 秒同步一次)都依然没有可用节点则退出
			// Determine whether the assigned downloader agent is active, because the database needs to synchronize data when switching occurs in the download center, then it is synchronized 2 times (15 seconds synchronization) and there are still no nodes available to exit.
			for (var i = 0; i < 35; ++i)
			{
				foreach (var agentId in agentIds)
				{
					if (Agents.ContainsKey(agentId) &&
						(DateTime.Now - Agents[agentId].LastModificationTime).TotalSeconds < 12)
					{
						onlineAgents.Add(Agents[agentId]);
					}
				}

				if (onlineAgents.Count == 0)
				{
					Thread.Sleep(1000);
				}
				else
				{
					break;
				}
			}

			if (onlineAgents.Count == 0)
			{
				// 直接退出即可。爬虫因为没有分配，触发无回应退出事件。
				Logger.LogError($"任务 {ownerId} 分配的下载器代理都已下线");
				return;
			}

			switch (allocateDownloaderMessage.DownloadPolicy)
			{
				case DownloadPolicy.Random:
					{
						foreach (var request in requests)
						{
							var agent = onlineAgents.Random();
							if (Dmq == null)
							{
								var json = JsonConvert.SerializeObject(new[] { request });
								var message = $"|{Framework.DownloadCommand}|{json}";
								await Mq.PublishAsync(agent.Id, message);
							}
							else
								await Dmq.PublishAsync(agent.Id, Framework.DownloadCommand, new[] { request });
						}

						break;
					}
				case DownloadPolicy.Chained:
					{
						foreach (var request in requests)
						{
							if (string.IsNullOrWhiteSpace(request.AgentId))
							{
								var agent = onlineAgents.Random();
								if (Dmq == null)
								{
									var json = JsonConvert.SerializeObject(new[] { request });
									var message = $"|{Framework.DownloadCommand}|{json}";
									await Mq.PublishAsync(agent.Id, message);
								}
								else
									await Dmq.PublishAsync(agent.Id, Framework.DownloadCommand, new[] { request });

							}
							else
							{
								var agent = onlineAgents.FirstOrDefault(x => x.Id == request.AgentId);
								if (agent == null)
								{
									Logger.LogError($"任务 {ownerId} 分配的下载器代理 {request.AgentId} 已下线");
								}
								else
								{
									if (Dmq == null)
									{
										var json = JsonConvert.SerializeObject(new[] { request });
										var message = $"|{Framework.DownloadCommand}|{json}";
										await Mq.PublishAsync(request.AgentId, message);
									}
									else
										await Dmq.PublishAsync(agent.Id, Framework.DownloadCommand, new[] { request });
								}
							}
						}

						break;
					}
			}
		}

		private Task StartSyncDownloaderAgentDataService()
		{
			return Task.Factory.StartNew(async () =>
			{
				while (_isRunning)
				{
					// 下载器代理的心跳时间
					Thread.Sleep(15000);

					await SyncDownloaderAgentData();
				}
			});
		}

		private async Task SyncDownloaderAgentData()
		{
			try
			{
				// 所有已经注册的下载器代理
				var agents = await DownloaderAgentStore.GetAllListAsync();
				foreach (var agent in agents)
				{
					// 如果不存在则添加到队列
					// 如果已经存在并且缓存的心跳时间小于数据库时间则更新缓存的心跳时间
					Agents.AddOrUpdate(agent.Id, x => agent, (agentId, cacheDownloaderAgent) =>
					{
						if (cacheDownloaderAgent.LastModificationTime < agent.LastModificationTime)
						{
							cacheDownloaderAgent.LastModificationTime = agent.LastModificationTime;
						}

						return cacheDownloaderAgent;
					});
				}

				Logger.LogInformation($"同步下载器代理数据成功: {agents.Count}");
			}
			catch (Exception e)
			{
				Logger.LogError($"同步下载器代理数据失败: {e}");
			}
		}
	}
}
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DotnetSpider.Core;
using DotnetSpider.Data;
using DotnetSpider.Data.Storage;
using DotnetSpider.Downloader;
using DotnetSpider.Downloader.Entity;
using DotnetSpider.MessageQueue;
using DotnetSpider.RequestSupply;
using DotnetSpider.Scheduler;
using DotnetSpider.Statistics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

[assembly: InternalsVisibleTo("DotnetSpider.Tests")]
[assembly: InternalsVisibleTo("DotnetSpider.Sample")]

namespace DotnetSpider
{
	/// <summary>
	/// Depth is a stand-alone system. Only a new request that is really parsed will cause Depth to be incremented.Depth can't be used as a Hash calculation for Request, because different depths will have the same link.
	/// Downloading and parsing causes retry without changing Depth, directly calling the download distribution service, skipping the Scheduler
	/// </summary>
	public partial class Spider
	{
		private readonly IServiceProvider _services;
		private readonly ISpiderOptions _options;

		/// <summary>
		/// Processing before the end
		/// </summary>
		/// <returns></returns>
		protected virtual Task OnExiting(IServiceProvider serviceProvider)
		{
#if NETFRAMEWORK
			return Framework.CompletedTask;
#else
			return Task.CompletedTask;
#endif
		}

		/// <summary>
		/// Spider Constructor
		/// </summary>
		/// <param name="mq"></param>
		/// <param name="options"></param>
		/// <param name="logger"></param>
		/// <param name="services">服务提供接口</param>
		/// <param name="statisticsService"></param>
		public Spider(
			IDynamicMessageQueue dmq,
			IMessageQueue mq,
			IStatisticsService statisticsService,
			ISpiderOptions options,
			ILogger<Spider> logger,
			IServiceProvider services)
		{
			_dmq = dmq;
			_services = services;
			_statisticsService = statisticsService;
			_mq = mq;
			_options = options;
			_logger = logger;
			Console.CancelKeyPress += ConsoleCancelKeyPress;
		}

		/// <summary>
		/// Create a Spider object
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		public static T Create<T>() where T : Spider
		{
			var builder = new SpiderBuilder();
			builder.AddSerilog();
			builder.ConfigureAppConfiguration();
			builder.UseStandalone();
			builder.AddSpider<T>();
			var factory = builder.Build();
			return factory.Create<T>();
		}

		/// <summary>
		/// Create New GUID for Id
		/// </summary>
		public Spider NewGuidId()
		{
			CheckIfRunning();
			Id = Guid.NewGuid().ToString("N");
			return this;
		}

		/// <summary>
		/// Add a request configuration method
		/// Can calculate cookies, Signs, etc
		/// </summary>
		/// <param name="configureDelegate">Configuration method</param>
		/// <returns></returns>
		public Spider AddConfigureRequestDelegate(Action<Request> configureDelegate)
		{
			Check.NotNull(configureDelegate, nameof(configureDelegate));
			_configureRequestDelegates.Add(configureDelegate);
			return this;
		}

		/// <summary>
		/// Add DataFlow
		/// </summary>
		/// <param name="dataFlow">数据流处理器</param>
		/// <returns></returns>
		public Spider AddDataFlow(IDataFlow dataFlow)
		{
			Check.NotNull(dataFlow, nameof(dataFlow));
			CheckIfRunning();
			dataFlow.Logger = _services.GetRequiredService<ILoggerFactory>().CreateLogger(dataFlow.GetType());
			_dataFlows.Add(dataFlow);
			return this;
		}

		/// <summary>
		/// Add Request Supply
		/// </summary>
		/// <param name="supply">请求供应器</param>
		/// <returns></returns>
		public Spider AddRequestSupply(IRequestSupply supply)
		{
			Check.NotNull(supply, nameof(supply));
			CheckIfRunning();
			_requestSupplies.Add(supply);
			return this;
		}

		/// <summary>
		/// Add Requests
		/// </summary>
		/// <param name="requests">请求</param>
		/// <returns></returns>
		[MethodImpl(MethodImplOptions.Synchronized)]
		public Spider AddRequests(params Request[] requests)
		{
			Check.NotNull(requests, nameof(requests));

			foreach (var request in requests)
			{
				request.OwnerId = Id;
				request.Depth = request.Depth == 0 ? 1 : request.Depth;
				_requests.Add(request);
				if (_requests.Count % EnqueueBatchCount == 0)
				{
					EnqueueRequests();
				}
			}

			return this;
		}

		/// <summary>
		/// Add Requests
		/// </summary>
		/// <param name="urls">链接</param>
		/// <returns></returns>
		[MethodImpl(MethodImplOptions.Synchronized)]
		public Spider AddRequests(params string[] urls)
		{
			Check.NotNull(urls, nameof(urls));

			foreach (var url in urls)
			{
				var request = new Request { Url = url, OwnerId = Id, Depth = 1, Method = HttpMethod.Get };
				_requests.Add(request);
				if (_requests.Count % EnqueueBatchCount == 0)
				{
					EnqueueRequests();
				}
			}

			return this;
		}

		/// <summary>
		/// Start crawler
		/// </summary>
		/// <param name="args">arguments</param>
		/// <returns></returns>
		public Task RunAsync(params string[] args)
		{
			CheckIfRunning();
			// This method can't be put into the asynchronous. If you call ExitBySignal directly after calling RunAsync, it may be called first to exit due to the execution order, and then the exit signal is reset.
			ResetMmfSignal();
			return Task.Factory.StartNew(async () =>
			{
				try
				{

					_logger.LogInformation("Initialize the crawler");
					// Initialization settings
					Initialize();

					// Set default scheduler
					_scheduler = _scheduler ?? new QueueDistinctBfsScheduler();

					// The setting status is: Run
					Status = Status.Running;

					// Add task-initiated monitoring information
					await _statisticsService.StartAsync(Id);

					// Subscribe to the data stream if the subscription fails
					_mq.Subscribe($"{Framework.ResponseHandlerTopic}{Id}",
						async message => await HandleMessage(message));
					// Subscribe to to handle dynamic ResponseHandler
					if (_dmq != null)
					{
						_dmq.Subscribe($"{Framework.ResponseHandlerTopic}{Id}",
							async (cmd, message) => await HandleDynamicMessage(cmd, message));
					}
					_logger.LogInformation($"Task {Id} subscribed to the message queue successfully");

					// If you have set up 10 downloaders to be assigned, the distribution is considered complete when 10 downloaders have been assigned.
					_allocated.Set(0);
					// If any of the downloader proxy assignments fail, directly assign this value to false after receiving the message, the crawler exits
					_allocatedSuccess = true;
					// The downloader is allocated first, because the overhead and time of the downloader are smaller, and then RequestSupply may load a large number of requests, which is expensive.
					await AllotDownloaderAsync();

					// Wait 30 seconds, if the allocation is not completed, the timeout ends.
					for (var i = 0; i < 200; ++i)
					{
						if (!_allocatedSuccess)
						{
							_logger.LogInformation($"Task {Id} failed to allocate downloader proxy");
							return;
						}

						if (_allocated.Value == DownloaderSettings.DownloaderCount)
						{
							_logger.LogInformation($"Task {Id} Assigned Downloader Agent Successful");
							break;
						}

						Thread.Sleep(150);
					}

					if (_allocated.Value == 0)
					{
						_logger.LogInformation($"Task {Id} failed to allocate downloader proxy");
						return;
					}

					// Add a request through the provisioning interface
					foreach (var requestSupply in _requestSupplies)
					{
						requestSupply.Run(request => AddRequests(request));
					}

					// Queue the possible remaining requests in the list
					EnqueueRequests();
					_logger.LogInformation($"Task {Id} loads the download request to end");

					// Initialize each Data Flow processor
					foreach (var dataFlow in _dataFlows)
					{
						await dataFlow.InitAsync();
					}

					_logger.LogInformation($"Task {Id} Data Flow processor initialization completed");
					_enqueued.Set(0);
					_responded.Set(0);
					_enqueuedRequestDict.Clear();

					// Start speed controller
					StartSpeedControllerAsync().ConfigureAwait(false).GetAwaiter();

					_lastRequestedTime = DateTime.Now;

					// Waiting for exit signal
					await WaitForExiting();
				}
				catch (Exception e)
				{
					_logger.LogError(e.ToString());
				}
				finally
				{
					foreach (var dataFlow in _dataFlows)
					{
						try
						{
							dataFlow.Dispose();
						}
						catch (Exception ex)
						{
							_logger.LogError($"任务 {Id} 释放 {dataFlow.GetType().Name} 失败: {ex}");
						}
					}

					try
					{
						// TODO: If the subscription to the message queue fails, whether it should be tried again here, it will cause twice the retry time. 
						// TODO: 如果订阅消息队列失败，此处是否应该再尝试上报，会导致两倍的重试时间
						// Add the monitoring information of the task exit.
						// 添加任务退出的监控信息
						await _statisticsService.ExitAsync(Id);

						// Last print task status information
						await _statisticsService.PrintStatisticsAsync(Id);
					}
					catch (Exception e)
					{
						_logger.LogInformation($"Task {Id} failed to upload and exit information: {e}");
					}

					try
					{
						await OnExiting(_services);
					}
					catch (Exception e)
					{
						_logger.LogInformation($"Task {Id} exit event processing failed: {e}");
					}

					// Identification task exit completed
					Status = Status.Exited;
					_logger.LogInformation($"Task {Id} exit");
				}
			});
		}

		/// <summary>
		/// Pause crawling
		/// </summary>
		public void Pause()
		{
			Status = Status.Paused;
		}

		/// <summary>
		/// Continue crawling
		/// </summary>
		public void Continue()
		{
			Status = Status.Running;
		}

		/// <summary>
		/// Exit the crawler
		/// </summary>
		public Spider Exit()
		{
			_logger.LogInformation($"Task {Id} is exiting...");
			Status = Status.Exiting;
			// Cancel your subscription directly: 1. If it is a local app
			_mq.Unsubscribe($"{Framework.ResponseHandlerTopic}{Id}");
			return this;
		}

		/// <summary>
		/// Send exit signal
		/// </summary>
		public Spider ExitBySignal()
		{
			if (MmfSignal)
			{
				var mmf = MemoryMappedFile.CreateFromFile(Path.Combine("mmf-signal", Id), FileMode.OpenOrCreate, null,
					4,
					MemoryMappedFileAccess.ReadWrite);
				using (var accessor = mmf.CreateViewAccessor())
				{
					accessor.Write(0, true);
					accessor.Flush();
				}

				_logger.LogInformation($"Task {Id} push exit signal to MMF success");
				return this;
			}

			throw new SpiderException($"Task {Id} does not turn on MMF control");
		}

		public void Run(params string[] args)
		{
			RunAsync(args).Wait();
		}

		/// <summary>
		/// Waiting for the end
		/// </summary>
		public void WaitForExit(long milliseconds = 0)
		{
			milliseconds = milliseconds <= 0 ? long.MaxValue : milliseconds;
			var waited = 0;
			while (Status != Status.Exited && waited < milliseconds)
			{
				Thread.Sleep(100);
				waited += 100;
			}
		}

		/// <summary>
		/// Initial configuration
		/// </summary>
		protected virtual void Initialize()
		{
		}

		/// <summary>
		/// Get data storage from configuration file
		/// </summary>
		/// <returns></returns>
		/// <exception cref="SpiderException"></exception>
		public StorageBase GetDefaultStorage()
		{
			return GetDefaultStorage(_options);
		}

		internal static StorageBase GetDefaultStorage(ISpiderOptions options)
		{
			var type = Type.GetType(options.Storage);
			if (type == null)
			{
				throw new SpiderException("The Storage type is not configured correctly, or the corresponding library is not added.");
			}

			if (!typeof(StorageBase).IsAssignableFrom(type))
			{
				throw new SpiderException("Storage type configuration is incorrect");
			}

			var method = type.GetMethod("CreateFromOptions");

			if (method == null)
			{
				throw new SpiderException("The Storage does not implement the CreateFromOptions method and cannot be created automatically");
			}

			var storage = method.Invoke(null, new object[] { options });
			if (storage == null)
			{
				throw new SpiderException("Failed to create default storage");
			}

			return (StorageBase)storage;
		}

		private void ResetMmfSignal()
		{
			if (MmfSignal)
			{
				if (!Directory.Exists("mmf-signal"))
				{
					Directory.CreateDirectory("mmf-signal");
				}

				var mmf = MemoryMappedFile.CreateFromFile(Path.Combine("mmf-signal", Id), FileMode.OpenOrCreate, null,
					4, MemoryMappedFileAccess.ReadWrite);
				using (var accessor = mmf.CreateViewAccessor())
				{
					accessor.Write(0, false);
					accessor.Flush();
					_logger.LogInformation($"Task {Id} initializes MMF exit signal");
				}
			}
		}

		/// <summary>
		/// Start speed controller
		/// </summary>
		/// <returns></returns>
		private Task StartSpeedControllerAsync()
		{
			return Task.Factory.StartNew(async () =>
			{
				bool @break = false;

				var mmf = MmfSignal
					? MemoryMappedFile.CreateFromFile(Path.Combine("mmf-signal", Id), FileMode.OpenOrCreate, null, 4,
						MemoryMappedFileAccess.ReadWrite)
					: null;

				using (var accessor = mmf?.CreateViewAccessor())
				{
					_logger.LogInformation($"Task {Id} speed controller starts");

					var paused = 0;
					while (!@break)
					{
						Thread.Sleep(_speedControllerInterval);

						try
						{
							switch (Status)
							{
								case Status.Running:
									{
										try
										{
											// Determine if too many download requests have not been answered
											if (_enqueued.Value - _responded.Value > NonRespondedLimitation)
											{
												if (paused > NonRespondedTimeLimitation)
												{
													_logger.LogInformation(
														$"Task {Id} {NonRespondedTimeLimitation} seconds did not receive a download response");
													@break = true;
													break;
												}

												paused += _speedControllerInterval;
												_logger.LogInformation($"Task {Id} Speed ​​Controller has not been suspended due to excessive download requests");
												continue;
											}

											paused = 0;

											// Retry timeout download request
											var timeoutRequests = new List<Request>();
											var now = DateTime.Now;
											foreach (var kv in _enqueuedRequestDict)
											{
												if (!((now - kv.Value.CreationTime).TotalSeconds > RespondedTimeout))
												{
													continue;
												}

												kv.Value.RetriedTimes++;
												if (kv.Value.RetriedTimes > RespondedTimeoutRetryTimes)
												{
													_logger.LogInformation(
														$"Task {Id} Retry Download Request {RespondedTimeoutRetryTimes} Not Received Download Response");
													@break = true;
													break;
												}

												timeoutRequests.Add(kv.Value);
											}

											// If there is a timeout download, try again, and no timeout download is taken from the dispatch queue.
											if (timeoutRequests.Count > 0)
											{
												await EnqueueRequests(timeoutRequests.ToArray());
											}
											else
											{
												var requests = _scheduler.Dequeue(Id, _dequeueBatchCount);

												if (requests == null || requests.Length == 0) break;

												foreach (var request in requests)
												{
													foreach (var configureRequestDelegate in _configureRequestDelegates)
													{
														configureRequestDelegate(request);
													}
												}

												await EnqueueRequests(requests);
											}
										}
										catch (Exception e)
										{
											_logger.LogError($"Task {Id} speed controller failed: {e}");
										}

										break;
									}
								case Status.Paused:
									{
										_logger.LogDebug($"Task {Id} speed controller paused");
										break;
									}
								case Status.Exiting:
								case Status.Exited:
									{
										@break = true;
										break;
									}
							}

							if (!@break && accessor != null && accessor.ReadBoolean(0))
							{
								_logger.LogInformation($"Task {Id} received MMF exit signal");
								Exit();
							}
						}
						catch (Exception e)
						{
							_logger.LogError($"Task {Id} speed controller failed: {e}");
						}
					}
				}

				_logger.LogInformation($"Task {Id} speed controller exit");
			});
		}

		/// <summary>
		/// Distribution downloader
		/// </summary>
		/// <returns>Whether the assignment is successful</returns>
		private async Task AllotDownloaderAsync()
		{
			var json = JsonConvert.SerializeObject(new AllocateDownloaderMessage
			{
				OwnerId = Id,
				AllowAutoRedirect = DownloaderSettings.AllowAutoRedirect,
				UseProxy = DownloaderSettings.UseProxy,
				DownloaderCount = DownloaderSettings.DownloaderCount,
				Cookies = DownloaderSettings.Cookies,
				DecodeHtml = DownloaderSettings.DecodeHtml,
				Timeout = DownloaderSettings.Timeout,
				Type = DownloaderSettings.Type,
				UseCookies = DownloaderSettings.UseCookies,
				CreationTime = DateTime.Now
			});
			await _mq.PublishAsync(Framework.DownloaderCenterTopic, $"|{Framework.AllocateDownloaderCommand}|{json}");
		}

		private async Task HandleMessage(string message)
		{
			if (string.IsNullOrWhiteSpace(message))
			{
				_logger.LogWarning($"Task {Id} received an empty message");
				return;
			}

			var commandMessage = message.ToCommandMessage();
			if (commandMessage != null)
			{
				switch (commandMessage.Command)
				{
					case Framework.AllocateDownloaderCommand:
						{
							if (commandMessage.Message == "true")
							{
								_allocated.Inc();
							}
							else
							{
								_logger.LogError($"Task {Id} failed to allocate downloader proxy");
								_allocatedSuccess = false;
							}

							break;
						}
					default:
						{
							_logger.LogError($"Task {Id} failed to process command: { message}");
							break;
						}
				}

				return;
			}

			_lastRequestedTime = DateTime.Now;

			Response[] responses;

			try
			{
				responses = JsonConvert.DeserializeObject<Response[]>(message);
			}
			catch
			{
				_logger.LogError($"Task {Id} received an exception message: {message}");
				return;
			}

			try
			{
				if (responses.Length == 0)
				{
					_logger.LogWarning($"Task {Id} received an empty reply");
					return;
				}

				_responded.Add(responses.Length);

				// As long as there is a response, it will be deleted from the cache. Even if the exception is to be re-downloaded, it will be added back to the cache in EnqueueRequest.
				// Here only need to ensure: Send -> Receive can be one-to-one delete to ensure the correctness of the detection mechanism
				foreach (var response in responses)
				{
					_enqueuedRequestDict.TryRemove(response.Request.Hash, out _);
				}

				var agentId = responses.First().AgentId;

				var successResponses = responses.Where(x => x.Success).ToList();
				// Statistical download success
				if (successResponses.Count > 0)
				{
					var elapsedMilliseconds = successResponses.Sum(x => x.ElapsedMilliseconds);
					await _statisticsService.IncrementDownloadSuccessAsync(agentId, successResponses.Count,
						elapsedMilliseconds);
				}

				// Handling a successful download request
				Parallel.ForEach(successResponses, async response =>
				{
					_logger.LogInformation($"Task {Id} Download {response.Request.Url} Success");

					try
					{
						var context = new DataFlowContext(response, _services.CreateScope().ServiceProvider);
						context["ProjectName"] = "Vnexpress.net";
						foreach (var dataFlow in _dataFlows)
						{
							var dataFlowResult = await dataFlow.HandleAsync(context);
							var @break = false;
							switch (dataFlowResult)
							{
								case DataFlowResult.Success:
									{
										continue;
									}
								case DataFlowResult.Failed:
									{
										// If the processing fails, return directly
										_logger.LogInformation($"Task {Id} failed to process {response.Request.Url}: {context.Result}");
										await _statisticsService.IncrementFailedAsync(Id);
										return;
									}
								case DataFlowResult.Terminated:
									{
										@break = true;
										break;
									}
							}

							if (@break)
							{
								break;
							}
						}

						var resultIsEmpty = !context.HasItems && !context.HasParseItems;
						// If the parsing result is empty, try again
						if (resultIsEmpty && RetryWhenResultIsEmpty)
						{
							if (response.Request.RetriedTimes < RetryDownloadTimes)
							{
								response.Request.RetriedTimes++;
								await EnqueueRequests(response.Request);

								// Now that the request is retried, the parsing will inevitably be executed again, so the resolved target link and success status should be processed at the end.
								_logger.LogInformation($"Task {Id} processing {response.Request.Url} parsing result is empty, try to try again.");
								return;
							}
						}

						// Parsed target request
						if (context.FollowRequests != null && context.FollowRequests.Count > 0)
						{
							var requests = new List<Request>();
							var currentPageIndex = 1;
							var requestPageIndexValue = context.Response.Request.GetProperty("PageIndex");
							if (!string.IsNullOrWhiteSpace(requestPageIndexValue))
							{
								currentPageIndex = int.Parse(requestPageIndexValue);
							}
							foreach (var followRequest in context.FollowRequests)
							{
								if (followRequest.PageIndex <= PageLimit)
								{
									// only increase Depth in case of page detail not Next Page.
									if (followRequest.PageIndex == currentPageIndex)
									{
										followRequest.Depth = response.Request.Depth + 1;
									}
									if (followRequest.Depth <= Depth)
									{
										requests.Add(followRequest);
									}
								}
							}

							var count = _scheduler.Enqueue(requests);
							if (count > 0)
							{
								await _statisticsService.IncrementTotalAsync(Id, count);
							}
						}

						if (!resultIsEmpty)
						{
							await _statisticsService.IncrementSuccessAsync(Id);
							_logger.LogInformation($"Task {Id} processed {response.Request.Url} successfully.");
						}
						else
						{
							if (RetryWhenResultIsEmpty)
							{
								await _statisticsService.IncrementFailedAsync(Id);
								_logger.LogInformation($"Task {Id} failed to process {response.Request.Url}, parsing result is empty.");
							}
							else
							{
								await _statisticsService.IncrementSuccessAsync(Id);
								_logger.LogInformation($"Task {Id} processed {response.Request.Url} succeeded, parsing result is empty.");
							}
						}
					}
					catch (Exception e)
					{
						await _statisticsService.IncrementFailedAsync(Id);
						_logger.LogInformation($"Task {Id} failed to process {response.Request.Url}: {e}");
					}
				});

				// TODO: 此处需要优化
				// Need to optimize here
				var retryResponses =
					responses.Where(x => !x.Success && x.Request.RetriedTimes < RetryDownloadTimes)
						.ToList();
				var downloadFailedResponses =
					responses.Where(x => !x.Success)
						.ToList();
				var failedResponses =
					responses.Where(x => !x.Success && x.Request.RetriedTimes >= RetryDownloadTimes)
						.ToList();

				if (retryResponses.Count > 0)
				{
					retryResponses.ForEach(x =>
					{
						x.Request.RetriedTimes++;
						_logger.LogInformation($"Task {Id} Download {x.Request.Url} failed: {x.Exception}");
					});
					await EnqueueRequests(retryResponses.Select(x => x.Request).ToArray());
				}

				// Statistical download failed
				if (downloadFailedResponses.Count > 0)
				{
					var elapsedMilliseconds = downloadFailedResponses.Sum(x => x.ElapsedMilliseconds);
					await _statisticsService.IncrementDownloadFailedAsync(agentId,
						downloadFailedResponses.Count, elapsedMilliseconds);
				}

				// Statistical failure
				if (failedResponses.Count > 0)
				{
					await _statisticsService.IncrementFailedAsync(Id, failedResponses.Count);
				}
			}
			catch (Exception ex)
			{
				_logger.LogError($"Task {Id} processing message {message} failed: {ex}");
			}
		}


		private async Task HandleDynamicMessage(string cmd, dynamic message)
		{


			_lastRequestedTime = DateTime.Now;

			Response[] responses;

			try
			{
				responses = message as Response[];// JsonConvert.DeserializeObject<Response[]>(message);
				if (responses == null)
				{
					var response = message as Response;
					if (response != null)
					{
						responses = new Response[] { response };
					}
				}
			}
			catch
			{
				_logger.LogError($"Task {Id} received an exception message: {message}");
				return;
			}

			try
			{
				if (responses == null || responses.Length == 0)
				{
					_logger.LogWarning($"Task {Id} received an empty reply");
					return;
				}

				_responded.Add(responses.Length);

				// As long as there is a response, it will be deleted from the cache. Even if the exception is to be re-downloaded, it will be added back to the cache in EnqueueRequest.
				// Here only need to ensure: Send -> Receive can be one-to-one delete to ensure the correctness of the detection mechanism
				foreach (var response in responses)
				{
					_enqueuedRequestDict.TryRemove(response.Request.Hash, out _);
				}

				var agentId = responses.First().AgentId;

				var successResponses = responses.Where(x => x.Success).ToList();
				// Statistical download success
				if (successResponses.Count > 0)
				{
					var elapsedMilliseconds = successResponses.Sum(x => x.ElapsedMilliseconds);
					await _statisticsService.IncrementDownloadSuccessAsync(agentId, successResponses.Count,
						elapsedMilliseconds);
				}

				// Handling a successful download request
				Parallel.ForEach(successResponses, async response =>
				{
					_logger.LogInformation($"Task {Id} Download {response.Request.Url} Success");

					try
					{
						var context = new DataFlowContext(response, _services.CreateScope().ServiceProvider);
						context["ProjectName"] = "Vnexpress.net";
						foreach (var dataFlow in _dataFlows)
						{
							var dataFlowResult = await dataFlow.HandleAsync(context);
							var @break = false;
							switch (dataFlowResult)
							{
								case DataFlowResult.Success:
									{
										continue;
									}
								case DataFlowResult.Failed:
									{
										// If the processing fails, return directly
										_logger.LogInformation($"Task {Id} failed to process {response.Request.Url}: {context.Result}");
										await _statisticsService.IncrementFailedAsync(Id);
										return;
									}
								case DataFlowResult.Terminated:
									{
										@break = true;
										break;
									}
							}

							if (@break)
							{
								break;
							}
						}

						var resultIsEmpty = !context.HasItems && !context.HasParseItems;
						// If the parsing result is empty, try again
						if (resultIsEmpty && RetryWhenResultIsEmpty)
						{
							if (response.Request.RetriedTimes < RetryDownloadTimes)
							{
								response.Request.RetriedTimes++;
								await EnqueueRequests(response.Request);

								// Now that the request is retried, the parsing will inevitably be executed again, so the resolved target link and success status should be processed at the end.
								_logger.LogInformation($"Task {Id} processing {response.Request.Url} parsing result is empty, try to try again.");
								return;
							}
						}

						// Parsed target request
						if (context.FollowRequests != null && context.FollowRequests.Count > 0)
						{
							var requests = new List<Request>();
							var currentPageIndex = 1;
							var requestPageIndexValue = context.Response.Request.GetProperty("PageIndex");
							if (!string.IsNullOrWhiteSpace(requestPageIndexValue))
							{
								currentPageIndex = int.Parse(requestPageIndexValue);
							}
							foreach (var followRequest in context.FollowRequests)
							{
								if (followRequest.PageIndex <= PageLimit)
								{
									// only increase Depth in case of page detail not Next Page.
									if (followRequest.PageIndex == currentPageIndex)
									{
										followRequest.Depth = response.Request.Depth + 1;
									}
									if (followRequest.Depth <= Depth)
									{
										requests.Add(followRequest);
									}
								}
							}

							var count = _scheduler.Enqueue(requests);
							if (count > 0)
							{
								await _statisticsService.IncrementTotalAsync(Id, count);
							}
						}

						if (!resultIsEmpty)
						{
							await _statisticsService.IncrementSuccessAsync(Id);
							_logger.LogInformation($"Task {Id} processed {response.Request.Url} successfully.");
						}
						else
						{
							if (RetryWhenResultIsEmpty)
							{
								await _statisticsService.IncrementFailedAsync(Id);
								_logger.LogInformation($"Task {Id} failed to process {response.Request.Url}, parsing result is empty.");
							}
							else
							{
								await _statisticsService.IncrementSuccessAsync(Id);
								_logger.LogInformation($"Task {Id} processed {response.Request.Url} succeeded, parsing result is empty.");
							}
						}
					}
					catch (Exception e)
					{
						await _statisticsService.IncrementFailedAsync(Id);
						_logger.LogInformation($"Task {Id} failed to process {response.Request.Url}: {e}");
					}
				});

				// TODO: 此处需要优化
				// Need to optimize here
				var retryResponses =
					responses.Where(x => !x.Success && x.Request.RetriedTimes < RetryDownloadTimes)
						.ToList();
				var downloadFailedResponses =
					responses.Where(x => !x.Success)
						.ToList();
				var failedResponses =
					responses.Where(x => !x.Success && x.Request.RetriedTimes >= RetryDownloadTimes)
						.ToList();

				if (retryResponses.Count > 0)
				{
					retryResponses.ForEach(x =>
					{
						x.Request.RetriedTimes++;
						_logger.LogInformation($"Task {Id} Download {x.Request.Url} failed: {x.Exception}");
					});
					await EnqueueRequests(retryResponses.Select(x => x.Request).ToArray());
				}

				// Statistical download failed
				if (downloadFailedResponses.Count > 0)
				{
					var elapsedMilliseconds = downloadFailedResponses.Sum(x => x.ElapsedMilliseconds);
					await _statisticsService.IncrementDownloadFailedAsync(agentId,
						downloadFailedResponses.Count, elapsedMilliseconds);
				}

				// Statistical failure
				if (failedResponses.Count > 0)
				{
					await _statisticsService.IncrementFailedAsync(Id, failedResponses.Count);
				}
			}
			catch (Exception ex)
			{
				_logger.LogError($"Task {Id} processing message {message} failed: {ex}");
			}
		}


		/// <summary>
		/// Block waiting until the crawler ends
		/// </summary>
		/// <returns></returns>
		private async Task WaitForExiting()
		{
			int waited = 0;
			while (Status != Status.Exiting)
			{
				if ((DateTime.Now - _lastRequestedTime).Seconds > EmptySleepTime)
				{
					break;
				}
				else
				{
					Thread.Sleep(1000);
				}

				waited += 1;
				if (waited > StatisticsInterval)
				{
					waited = 0;
					await _statisticsService.PrintStatisticsAsync(Id);
				}
			}
		}

		/// <summary>
		/// Determine if the crawler is running
		/// </summary>
		private void CheckIfRunning()
		{
			if (Status == Status.Running || Status == Status.Paused)
			{
				throw new SpiderException($"Task {Id} is running");
			}
		}

		private void ConsoleCancelKeyPress(object sender, ConsoleCancelEventArgs e)
		{
			Exit();
			while (Status != Status.Exited)
			{
				Thread.Sleep(100);
			}
		}

		/// <summary>
		/// Enqueue all currently cached Requests
		/// </summary>
		[MethodImpl(MethodImplOptions.Synchronized)]
		private void EnqueueRequests()
		{
			if (_requests.Count <= 0) return;

			_scheduler = _scheduler ?? new QueueDistinctBfsScheduler();

			var count = _scheduler.Enqueue(_requests);
			_statisticsService.IncrementTotalAsync(Id, count).ConfigureAwait(false).GetAwaiter();
			_logger.LogInformation($"Task {Id} push request to scheduler, quantity: {_requests.Count}");
			_requests.Clear();
		}

		private async Task EnqueueRequests(params Request[] requests)
		{
			if (requests.Length > 0)
			{
				foreach (var request in requests)
				{
					request.CreationTime = DateTime.Now;
				}

				if (_dmq == null)
				{
					await _mq.PublishAsync(Framework.DownloaderCenterTopic,
						$"|{Framework.DownloadCommand}|{JsonConvert.SerializeObject(requests)}");
				}
				else
					await _dmq.PublishAsync(Framework.DownloaderCenterTopic, Framework.DownloadCommand, requests);
				foreach (var request in requests)
				{
					_enqueuedRequestDict.TryAdd(request.Hash, request);
				}

				_enqueued.Add(requests.Length);
			}
		}
	}
}
using System;
using System.Threading.Tasks;

namespace DotnetSpider.MessageQueue
{
	/// <summary>
	/// 消息队列接口
	/// 因为业务上所有定阅都不需要负载，因此不存在多个客户端订阅同一个 topic 的情况，不需要 Unsubscribe 的实现
	/// </summary>
	public interface IMessageQueue
	{
		/// <summary>
		/// 推送消息到指定 topic
		/// </summary>
		/// <param name="topic"></param>
		/// <param name="messages"></param>
		/// <returns></returns>
		Task PublishAsync(string topic, params string[] messages);

		/// <summary>
		/// 订阅 topic
		/// </summary>
		/// <param name="topic"></param>
		/// <param name="action"></param>
		void Subscribe(string topic, Func<string, Task> action);

		/// <summary>
		/// 取消订阅 topic
		/// </summary>
		/// <param name="topic"></param>
		void Unsubscribe(string topic);
	}

	/// <summary>
	/// A MessageQueue with dynamic parameters
	/// </summary>
	public interface IDynamicMessageQueue
	{
		/// <summary>
		/// 推送消息到指定 topic
		/// </summary>
		/// <param name="topic">topic</param>
		/// <param name="cmd">command</param>
		/// <param name="messages">messages</param>
		/// <returns></returns>
		Task PublishAsync(string topic, string cmd, params dynamic[] messages);

		/// <summary>
		/// 订阅 topic
		/// </summary>
		/// <param name="topic"></param>
		/// <param name="action"></param>
		void Subscribe(string topic, Func<string,dynamic, Task> action);

		/// <summary>
		/// 取消订阅 topic
		/// </summary>
		/// <param name="topic"></param>
		void Unsubscribe(string topic);
	}
}
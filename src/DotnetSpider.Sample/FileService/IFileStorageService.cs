using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace DotnetSpider.Sample.FileService
{
	public interface IFileStorageService
	{
		Task<bool> ExistsAsync(string folder, string file);
		Task<string> GetAsync(string folder, string file);
		Task<bool> SaveAsync(string folder, string file, Stream content);
		Task<bool> SaveAsync(string folder, string file, string content);
	}
}

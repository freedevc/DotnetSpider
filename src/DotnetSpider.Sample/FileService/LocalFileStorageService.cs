using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace DotnetSpider.Sample.FileService
{
	public class LocalFileStorageService : IFileStorageService
	{
		public async Task<bool> ExistsAsync(string folder, string file)
		{
			if (string.IsNullOrWhiteSpace(file) || string.IsNullOrWhiteSpace(folder))
				throw new ArgumentNullException("Argument 'folder' and 'file' cannot be null");

			return await Task.FromResult<bool>(File.Exists(Path.Combine(folder, file))).ConfigureAwait(false);
		}


		public async Task<string> GetAsync(string folder, string file)
		{
			if(await ExistsAsync(folder, file).ConfigureAwait(false))
			{
#if NETFRAMEWORK
				var content =File.ReadAllText(Path.Combine(folder, file));
				return await Task.FromResult<string>(content).ConfigureAwait(false);
#else
				return await File.ReadAllTextAsync(Path.Combine(folder, file)).ConfigureAwait(false);
#endif

			}
			return await Task.FromResult<string>(null);
		}


		public async Task<bool> SaveAsync(string folder, string file, Stream content)
		{
			if (string.IsNullOrWhiteSpace(file) || string.IsNullOrWhiteSpace(folder))
				throw new ArgumentNullException("Argument 'folder' and 'file' cannot be null");
			if (!Directory.Exists(folder))
				Directory.CreateDirectory(folder);
			using (var fstream = File.Open(Path.Combine(folder, file), FileMode.Create))
			{
				await content.CopyToAsync(fstream).ConfigureAwait(false);
			}
			return await Task.FromResult<bool>(true);
		}

		public async Task<bool> SaveAsync(string folder, string file, string content)
		{
			if (string.IsNullOrWhiteSpace(file) || string.IsNullOrWhiteSpace(folder))
				throw new ArgumentNullException("Argument 'folder' and 'file' cannot be null");
			if (!Directory.Exists(folder))
				Directory.CreateDirectory(folder);
#if NETFRAMEWORK
			File.WriteAllText(Path.Combine(folder, file), content);
#else
			await File.WriteAllTextAsync(Path.Combine(folder, file), content).ConfigureAwait(false);
#endif

			return await Task.FromResult<bool>(true).ConfigureAwait(false);
		}

		
	}
}

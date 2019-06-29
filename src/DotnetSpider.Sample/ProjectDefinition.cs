using DotnetSpider.Sample.Parsers;
using System;
using System.Collections.Generic;
using System.Text;

namespace DotnetSpider.Sample
{
	public class ProjectDefinition
	{
		public string ProjectName { get; set; }
		public string Site { get; set; }
		public string FileFormat { get; set; }
		public string FileStorage { get; set; }
		public string Urls { get; set; }
		public string NextPageSelector { get; set; }
		public string ItemUrlsSelector { get; set; }
		public int? Deepth { get; set; }
		public int? PageLimit { get; set; }
		public int NumberOfConcurrentRequests { get; set; }
		public ItemMapping Mapping {get;set;}
	}
}

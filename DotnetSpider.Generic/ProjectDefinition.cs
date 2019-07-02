using System;
using System.Collections.Generic;
using System.Text;

namespace DotnetSpider.Generic
{
	public class ProjectDefinition
	{
		public string ProjectName { get; set; }
		public string Site { get; set; }
		public string FileFormat { get; set; }
		public string DestinationFolder { get; set; }
		public string Urls { get; set; }
		public string NextPageSelector { get; set; }
		public string ItemUrlsSelector { get; set; }
		public int? Deepth { get; set; }
		public int? PageLimit { get; set; }
		public int NumberOfConcurrentRequests { get; set; }
		public ItemMapping Mapping { get; set; }
		public const string PageIndexKey = "64D0C4067C6246B29BA4B19009929B3C";
		public string Proxies { get; set; }
		public string ExtractType { get; set; }
		
	}

	public class ItemMapping
	{
		public ItemMapping() { }
		public string ItemCssSelector { get; set; }
		public FieldMapping[] Mapping { get; set; }
		/// <summary>
		/// Only extract item details if a page is at exactly Deepth (start from 1)
		/// </summary>
		public int? OnlyPagesAtDepth { get; set; }
	}

	public class FieldMapping
	{
		public string Field { get; set; }
		public string CssSelector { get; set; }
	}
}

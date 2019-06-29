using System;
using System.Collections.Generic;
using System.Text;

namespace DotnetSpider.Sample.Parsers
{
	public class ItemMapping
	{
		public ItemMapping() {  }
		public string ItemCssSelector { get; set; }
		public FieldMapping[] Mapping { get; set; } 
		/// <summary>
		/// Only extract item details if a page is at exactly Deepth (start from 1)
		/// </summary>
		public int? Deepth { get; set; }
	}

	public class FieldMapping
	{
		public string Field { get; set; }
		public string CssSelector { get; set; }
	}
}

using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace DescribeImageFunction.Models
{

    public class AnalyzedModel
    {

        [JsonProperty("description")]
        public AnalyzedDescription Description { get; set; }


    }

    public class AnalyzedDescription
    {

        [JsonProperty("captions")]
        public List<AnalyzedCaption> Captions { get; set; }

    }

    public class AnalyzedCaption
    {

        [JsonProperty("text")]
        public string Text { get; set; }

    }
}

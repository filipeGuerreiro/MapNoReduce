using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using MapNoReduce;

namespace MapLib {

    [Serializable]
    /// <summary>
    /// Mapper class.
    /// 
    /// Implements a word-count map function.
    /// </summary>
    public class Mapper : IMapper {

        public IList<KeyValuePair<string, string>> Map(string fileLine) {
            IList<KeyValuePair<String, String>> result = new List<KeyValuePair<String, String>>();

            string[] splitWords = fileLine.Split(new char[] { '.', '?', '!', ' ', ';', ':', ',' },
                StringSplitOptions.RemoveEmptyEntries);

            HashSet<string> set = new HashSet<string>(splitWords);
            string[] noDuplicateWords = new string[set.Count];
            set.CopyTo(noDuplicateWords);

            try {
                foreach (string word in noDuplicateWords) {
                    int wordCount = Regex.Matches(fileLine, word).Count;
                    result.Add(new KeyValuePair<String, String>(word, wordCount.ToString()));
                }
            } catch (Exception) {
                // Do not add line to result
            }

            return result;
        }
    }
}

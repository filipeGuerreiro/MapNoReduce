using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommonLib {
    public interface IClient {
        void AsyncWriteResult(IList<IList<KeyValuePair<string, string>>> result,
            IList<split> splitsToFetch, string outputDir);
    }
}

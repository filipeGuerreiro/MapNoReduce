using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommonLib {

    /// <summary>
    /// ClientService class.
    /// 
    /// This class is in charge of exposing to the user
    /// application the method to submit jobs to the system.
    /// It is also responsible for providing the splits to
    /// the workers, and merge the results obtained from them.
    /// </summary>
    public class ClientService : Service {

        private IClient client;

        private IList<string> inputFile = new List<string>();
        public IList<string> InputFile {
            get { return inputFile; }
            set { inputFile = value; }
        }

        private string outputDir;
        public string OutputDir {
            get { return outputDir; }
            set { outputDir = value; }
        }

        public ClientService(IClient client) {
            this.client = client;
        }

        // 
        public IList<string> GetSplit(split split) {
            IList<string> ret = new List<string>();

            // uses the lines between l[0] (inclusive) and l[1] (exclusive)
            for (int i = split.from; i <= split.to; i++) {

                // just in case
                if (i >= inputFile.Count) {
                    break;
                }
                ret.Add(inputFile[i]);
            }

            return ret;
        }

        /// <summary>
        /// Writes the final result back to the client in the address given in 'outputDir'.
        /// The file names will be in the format 'splitkey.out', e.g. '1.out' '2.out'
        /// </summary>
        /// <param name="result"></param>
        public void WriteFinalResult(IList<IList<KeyValuePair<string, string>>> result,
                IList<split> splitsToFetch) {

            client.AsyncWriteResult(result, splitsToFetch, outputDir);
        }

    }
}

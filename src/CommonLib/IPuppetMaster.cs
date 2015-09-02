using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommonLib {
    public interface IPuppetMaster {

        LoggerWrapper GetLogger();

        IWorker CreateWorker(int id, string serviceURL, string entryURL = null);
    }
}

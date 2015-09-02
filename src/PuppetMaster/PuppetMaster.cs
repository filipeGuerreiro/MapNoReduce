using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using CommonLib;
using WorkerLib;

namespace PuppetMaster {
    class PuppetMaster : IPuppetMaster {

        private LoggerWrapper logger;

        public PuppetMaster(string name) {
            logger = new LoggerWrapper(name);
        }

        public LoggerWrapper GetLogger() {
            return logger;
        }

        public IWorker CreateWorker(int id, string serviceURL, string entryURL = null) {
            Worker worker = new Worker(id, serviceURL, entryURL);
            worker.InitWorker();

            return worker;
        }

    }
}

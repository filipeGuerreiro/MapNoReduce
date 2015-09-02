using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization.Formatters;

using CommonLib;

namespace WorkerLib {

    public class Tracker {

        //private int port;
        private string workerEntryURL;
        private string workerServiceURL;

        private LoggerWrapper logger;

        private int workerId;

        public Thread jobTrackerThread;

        public Thread JobTrackerThread {
            get { return jobTrackerThread; }
        }

        public Tracker(int id, string serviceURL, string entryURL = null) {
            this.workerId = id;
            this.workerEntryURL = entryURL;
            this.workerServiceURL = serviceURL;

        }

        public void InitTracker(LoggerWrapper logger) {

            this.logger = logger;

            jobTrackerThread = new Thread(new ThreadStart(TrackerJob));
            jobTrackerThread.Start();
        }


        private void TrackerJob() {

            logger.Log("Job tracker is active on worker with id:" + workerId + " at " + workerServiceURL);
            System.Console.ReadLine();
        }

        /*private void updateMasterTracker(string entryURL) {
            //TODO:
            WorkerService workerService =
                (WorkerService)Factory.Instance.GetService(
                    typeof(WorkerService), entryURL);

        }*/
    }
}

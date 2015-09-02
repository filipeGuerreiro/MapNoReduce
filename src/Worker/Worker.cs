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

using System.Text.RegularExpressions;
using System.Net.Sockets;

using CommonLib;
using MapNoReduce;

namespace WorkerLib {

    public delegate void WorkerStatusDelegate(int id, string status);

    public delegate void WorkerDied(int id);

    public class Worker : IWorker {

        public const int SEND_RESULTS_TIMEOUT = 10000;

        private int port;
        private string entryURL;
        private string serviceURL;

        private WorkerService masterTrackerFather = null;
        public WorkerService MasterTrackerFather {
            get { return masterTrackerFather; }
        }

        private WorkerService masterTrackerService = null;
        public WorkerService MasterTrackerService {
            get { return masterTrackerService; }
        }

        private int masterTrackerId = -1;

        private LoggerWrapper logger;

        private static ManualResetEvent freezeWorkerEvent = new ManualResetEvent(true);

        private WorkerDied workerDied;
        public event WorkerDied WorkerDiedEvent {
            add { workerDied += value; }
            remove { if (workerDied != null) workerDied -= value; }
        }

        private int id;
        public int Id {
            get { return id; }
        }

        // Read-only
        private WorkerService service;
        public WorkerService Service {
            get { return service; }
        }

        private workerContext context;
        public workerContext Context {
            get { return context; }
        }

        // Submit Controller Info
        private bool resumeOperation = false;
        private IList<split> controllerSplitsToFetch;
        private List<string> controllerFileObtained;

        // Controller Output Splits
        private Dictionary<int, IList<KeyValuePair<string, string>>> controllerMapResult
            = new Dictionary<int, IList<KeyValuePair<string, string>>>();

        // Submit results
        private List<string> fileObtained;
        private IList<IList<KeyValuePair<string, string>>> mapResult;
        private int nSplitsProcessed = 0;

        private Thread workerThread;
        public Thread WorkerThread {
            get { return workerThread; }
        }

        private enum JOB_STATUS { IDLE, ASK_INPUT, TRANSFER_OUTPUT, COMPLETED, COMPUTE_MAP, COMPUTE_DONE };
        private JOB_STATUS status;
        public string Status {
            get { return status.ToString(); }
        }

        private static WorkerStatusDelegate onStatusCallbacks;
        public static event WorkerStatusDelegate OnStatus {
            add { onStatusCallbacks += value; }
            remove { if (onStatusCallbacks != null) onStatusCallbacks -= value; }
        }

        private const int TIME_BETWEEN_LIFEPROOFS = 5000;
        private System.Timers.Timer lifeProofTimer;

        public Worker(int id, string serviceURL, string entryURL = null) {
            this.id = id;
            this.serviceURL = serviceURL;
            this.entryURL = entryURL;
        }

        public void InitWorker() {
            logger = new LoggerWrapper("W" + id);

            Regex regex = RegexConstants.PATTERN_PORT;
            var v = regex.Match(serviceURL);
            port = int.Parse(v.Groups[1].ToString());

            context = new workerContext(new Object());

            setupService();

            // Notify Master Tracker that I'm a new worker
            if (entryURL != null) {
                notifyMasterTracker(entryURL);
            }
        }

        public void SetMasterTracker(WorkerService service) {
            this.masterTrackerService = service;
            try {
                masterTrackerId = service.Id;
            } catch (Exception) {
                masterTrackerId = -1;
            }
        }

        public WorkerService GetMasterTracker() {
            try {
                masterTrackerService.ProbeObject();
                return masterTrackerService;
            } catch (Exception) {
                return null;
            }
        }

        public void SetMasterTrackerFather(WorkerService service) {
            this.masterTrackerFather = service;
        }

        public WorkerService GetMasterTrackerFather() {
            try {
                masterTrackerFather.ProbeObject();
                return masterTrackerFather;
            } catch (Exception) {
                return null;
            }
        }

        public void PropagateWorker(WorkerService worker) {
            if (masterTrackerService != null) {
                Factory.Instance.SafelyUseService(masterTrackerService, (service) => {
                    service.AddWorker(worker, true);
                });
            }
        }

        // --------------------------------------- IWorker Implementation //

        // Function that get the worker service url
        public WorkerService GetService() {
            return service;
        }

        // Function that get the worker id
        public int GetId() {
            return id;
        }

        public void SetOperationSubmit(string clientURL, IMapper submitedMap, IList<split> splitsToFetch, int workerControllerId) {
            context.Operation = WORKER_OPERATION.SUBMIT;
            context.ClientURL = clientURL;
            context.SubmitedMap = submitedMap;
            context.SplitsToFetch = splitsToFetch;
            context.workerControllerId = workerControllerId;
            this.nSplitsProcessed = 0;
            pulseLock(context.MainLock);
        }

        public void ResumeOperationSubmit(string clientURL, IMapper submitedMap, IList<split> splitsToFetch) {
            context.Operation = WORKER_OPERATION.SUBMIT;
            context.ClientURL = clientURL;
            context.SubmitedMap = submitedMap;

            if (this.controllerSplitsToFetch != null && this.controllerFileObtained != null) {
                context.SplitsToFetch = this.controllerSplitsToFetch;
                fileObtained = this.controllerFileObtained;
                resumeOperation = true;
            } else {
                context.SplitsToFetch = splitsToFetch;

            }

            pulseLock(context.MainLock);
        }

        public string GetStatus() {
            return Status;
        }

        public void ShowStatus() {
            string res = "";


            string listWorkersString = "";
            foreach (String worker in service.ListWorkers) {
                listWorkersString += worker + "\r\n";
            }
            res += "---TRACKING--- \r\n" + listWorkersString + "STATUS: " + Status + "\r\n";
            if (context.SplitsToFetch != null) {
                res += "---PROGRESS--- \r\n" + "DONE: " + this.nSplitsProcessed + " TOTAL: " + context.SplitsToFetch.Count;
            }
            logger.Log(res);
        }

        public void Die() {
            context.Operation = WORKER_OPERATION.EXIT;

            service.DisposeWorker(id);
            Factory.Instance.DestroyService(service);

            if (lifeProofTimer != null) {
                lifeProofTimer.Dispose();
            }


            if (workerDied != null) {
                workerDied(id);
            }
        }

        // --------------------------------------- IWorker Implementation //

        public bool IsMasterTracker() {
            try {
                if (masterTrackerService != null) {
                    masterTrackerService.ProbeObject();
                }
            } catch (Exception) {
                masterTrackerService = masterTrackerFather;
                masterTrackerFather = null;
                return IsMasterTracker();
            }
            return masterTrackerService == null;
        }

        private void waitLock(Object lockObj, int slowTime = -1) {
            Monitor.Enter(lockObj);
            try {
                if (slowTime < 0) {
                    Monitor.Wait(lockObj);
                } else {
                    Monitor.Wait(lockObj, slowTime + 1);
                }

            } finally {
                Monitor.Exit(lockObj);
            }
        }

        private void pulseLock(Object pulseObj) {
            Monitor.Enter(pulseObj);
            try {
                Monitor.Pulse(pulseObj);
            } finally {
                Monitor.Exit(pulseObj);
            }
        }

        private void changeStatus(JOB_STATUS status) {
            this.status = status;
            if (onStatusCallbacks != null) {
                onStatusCallbacks(id, Status);
            }
        }

        private void setupService() {
            string serviceName = Constants.WORKER_SERVICE_NAME;
            service = new WorkerService(this);

            if (Factory.Instance.CreateService(port, serviceName,
                    service, typeof(WorkerService), serviceURL)) {

                logger.Log("I'm a new worker at " + serviceURL);

                workerThread = new Thread(new ThreadStart(WorkerJob));
                workerThread.Start();
            } else {
                logger.Log("Can'start worker at " + serviceURL);
            }
        }

        private void notifyMasterTracker(string entryURL) {
            WorkerService tracker =
                (WorkerService)Factory.Instance.GetService(
                    typeof(WorkerService), entryURL);

            tracker.AddWorker(this.service);

            masterTrackerFather = tracker.GetMasterTracker();

            lifeProofTimer = createSendLifeproofTimer(masterTrackerService);
        }

        private System.Timers.Timer createSendLifeproofTimer(WorkerService tracker) {
            System.Timers.Timer newTimer = new System.Timers.Timer();
            newTimer.Elapsed += (sender, args) => {
                SendLifeproof(tracker);
            };
            newTimer.Interval = TIME_BETWEEN_LIFEPROOFS;
            newTimer.Enabled = true;

            return newTimer;
        }

        private void SendLifeproof(object tracker) {
            WorkerService masterTracker = (WorkerService)tracker;

            try {
                SetMasterTrackerFather(masterTracker.ReceiveLifeproof(id, service));
                //logger.Log("Sent lifeproof to W" + masterTrackerId);

                if (lifeProofTimer != null) {
                    lifeProofTimer.Stop();
                    lifeProofTimer.Start();
                } else {
                    lifeProofTimer = createSendLifeproofTimer(masterTracker);
                }
            } catch (Exception) {
                logger.Log("Master Tracker not responding!");

                if (masterTrackerId != -1) {
                    service.WorkerFailureHandle(new WorkerService.FaultyWorkerEventArgs(masterTrackerId));
                }

                try {
                    lifeProofTimer.Dispose();
                } catch (Exception) {
                    // some error goes here when timer was already disposed???
                }

                if (masterTrackerFather != null) {
                    masterTrackerService = masterTrackerFather;
                    masterTrackerFather = null;
                    try {
                        SetMasterTrackerFather(masterTrackerService.ReceiveLifeproof(id, service));
                    } finally {
                        lifeProofTimer = createSendLifeproofTimer(masterTrackerService);
                    }
                }
            }
        }

        private void WorkerJob() {
            do {
                switch (context.Operation) {
                    case WORKER_OPERATION.SUBMIT:
                        processSubmit();
                        break;
                    case WORKER_OPERATION.EXIT:
                        // tear down stuff
                        return;
                }
                waitLock(context.MainLock);
            } while (!context.Operation.Equals(WORKER_OPERATION.EXIT));
        }

        public void SetControlledWorkerInfo(int controlledId, IList<split> splits, List<string> FileObtained) {

            if (FileObtained != null) {
                this.controllerFileObtained = FileObtained;
            }

            if (splits != null) {
                this.controllerSplitsToFetch = splits;
            }

            if (splits.Count == 0) {
                logger.Log(@"\\\\\\\\\\\\\\" + " WORKER: " + controlledId + " HAS DONE ALL SPLITS!");
            } /*else {
                logger.Log(@"\\\\\\\\\\\\\\" + " WORKER: " + controlledId + " HAS MORE " + splits.Count + " SPLITS TO DO!");
            }*/
        }

        public void SetControlledWorkerOutput(int controlledId, Dictionary<int, IList<KeyValuePair<string, string>>> mapResult) {
            int init = int.MaxValue;
            int count = 0;
            foreach (var entry in mapResult) {
                if (!controllerMapResult.ContainsKey(entry.Key)) {
                    if (entry.Key < init) {
                        init = entry.Key;
                    }
                    count++;
                    controllerMapResult.Add(entry.Key, entry.Value);
                }
            }
            logger.Log(@"//////////////" + " WORKER: " + controlledId + " HAS SENT RESULT FROM LINES "
                + init + " - " + (init + count) + "!");
        }

        /// <summary>
        /// This method is responsible for obtaining the 
        /// part of the file the worker is responsible for processing.
        /// </summary>
        private void askClientForFile(ClientService clientService) {

            logger.Log("Asking client '" + context.ClientURL + "' for file fragments.");

            List<string> jobFileLines = new List<string>();
            int fetchedSplits = 0;
            foreach (split job in context.SplitsToFetch) {
                fetchedSplits++;
                IList<string> fileFragment = clientService.GetSplit(job);

                // prints the returned splits (Debug only)
                int printedLines = 0;
                foreach (string line in fileFragment) {
                    if (fetchedSplits < 3) {
                        if (printedLines < 3) {
                            logger.Log("Got: '" + line);
                            printedLines++;
                        } else if (printedLines == 3) {
                            logger.Log(" ==> ... Fetching more " + (job.to - job.from - 3) + " lines ...");
                            printedLines++;
                        }
                    } else if (fetchedSplits == 3) {
                        logger.Log(" ==> ... More " + (context.SplitsToFetch.Count - 3) + " fragments to do ...");
                        fetchedSplits++;
                    }
                }
                // ---

                // adds lines to return value
                jobFileLines = jobFileLines.Concat(fileFragment).ToList();
            }

            fileObtained = jobFileLines;
        }

        /// <summary>
        /// Does the map work and store the result at mapResult private field.
        /// </summary>
        private void doMapJob(ClientService clientService) {
            logger.Log("Got Mapper class: " + context.SubmitedMap.GetType());

            string trackerURL = "";

            if (context.workerControllerId > 10)
                trackerURL = "tcp://localhost:" + 300 + context.workerControllerId + "/W";
            else if (context.workerControllerId > 100)
                trackerURL = "tcp://localhost:" + 30 + context.workerControllerId + "/W";
            else if (context.workerControllerId < 10)
                trackerURL = "tcp://localhost:" + 3000 + context.workerControllerId + "/W";

            WorkerService tracker =
                (WorkerService)Factory.Instance.GetService(
                    typeof(WorkerService), trackerURL);

            if (tracker != null) {
                Factory.Instance.SafelyUseService(tracker, (service) => {
                    service.UpdateSubmit(id, context.SplitsToFetch, fileObtained);
                });
            }

            List<split> newSplitsToFetch = new List<split>(context.SplitsToFetch);

            mapResult = new List<IList<KeyValuePair<string, string>>>();
            int j = 0;
            int fetchedSplits = 0;
            foreach (split job in context.SplitsToFetch) {
                fetchedSplits++;
                int printedLines = 0;
                mapResult.Add(new List<KeyValuePair<string, string>>());

                Dictionary<int, IList<KeyValuePair<string, string>>> sendResult
                    = new Dictionary<int, IList<KeyValuePair<string, string>>>();

                int timeMilli = 0;
                int lastTimeMilli = System.DateTime.Now.Millisecond;
                for (int i = job.from; i <= job.to; i++) {
                    int currentTimeMilli = System.DateTime.Now.Millisecond;

                    timeMilli += currentTimeMilli - lastTimeMilli;

                    IList<KeyValuePair<string, string>> tempResult = null;

                    // If other worker calculated the result use it
                    if (sendResult.ContainsKey(i)) {
                        tempResult = sendResult[i];
                    } else {
                        // No value calculated, do the hard work
                        tempResult = context.SubmitedMap.Map(fileObtained[i - job.from]);
                    }
                    mapResult[j] = mapResult[j].Concat(tempResult).ToList();
                    sendResult.Add(i, mapResult[j]);

                    if (timeMilli > SEND_RESULTS_TIMEOUT) {
                        logger.Log("Sending calculated result to tracker");
                        if (tracker != null) {
                            Factory.Instance.SafelyUseService(tracker, (service) => {
                                service.UpdateOutputSplit(id, sendResult);
                            });
                        }
                        sendResult.Clear();
                    }

                    lastTimeMilli = currentTimeMilli;

                    // prints the map result splits (Debug only)
                    if (fetchedSplits < 3) {
                        if (printedLines < 3) {
                            logger.Log("Result of Map[" + i + "] is: ");
                            printedLines++;
                            foreach (KeyValuePair<string, string> value in tempResult) {
                                logger.Log("--- " + value);
                            }
                        } else if (printedLines == 3) {
                            logger.Log(" ==> ... More " + (job.to - job.from - 3) + " results ...");
                            printedLines++;
                        }
                    } else if (fetchedSplits == 3) {
                        logger.Log(" ==> ... More " + (context.SplitsToFetch.Count - 3) + " splits to do ...");
                        fetchedSplits++;
                    }
                    // ---
                }
                j++;

                List<split> splitToResult = new List<split>();

                splitToResult.Add(job);

                clientService.WriteFinalResult(mapResult, splitToResult);
                this.nSplitsProcessed++;

                newSplitsToFetch.Remove(job);

                if (tracker != null) {
                    Factory.Instance.SafelyUseService(tracker, (service) => {
                        service.UpdateSubmit(id, newSplitsToFetch, null);
                    });
                }
            }
        }

        /// <summary>
        /// Processes the submit command using the loaded metadata.
        /// </summary>
        private void processSubmit() {

            ClientService clientService =
                (ClientService)Factory.Instance.GetService(
                    typeof(ClientService), context.ClientURL);

            // Asks the client the corresponding splits
            changeStatus(JOB_STATUS.ASK_INPUT);

            if (!resumeOperation)
                askClientForFile(clientService);

            // Does the Map operations
            changeStatus(JOB_STATUS.COMPUTE_MAP | JOB_STATUS.TRANSFER_OUTPUT);
            doMapJob(clientService);
            //changeStatus(JOB_STATUS.COMPUTE_DONE);

            // Returns the output to client
            //changeStatus(JOB_STATUS.TRANSFER_OUTPUT);
            //clientService.WriteFinalResult(mapResult, context.SplitsToFetch);
            changeStatus(JOB_STATUS.COMPLETED);
            logger.Log("Wrote result to client");

            // Notifies tracker that it is done
            try {
                if (!IsMasterTracker()) { // if it is not a master tracker
                    masterTrackerService.ReceiveWorkDoneNotify(id);
                } else {
                    service.ReceiveWorkDoneNotify(id);
                }
            } catch (Exception) {
                // If communication with other workers
                // is down we do not want to stop
            }

            resumeOperation = false;
        }

        private string parseServiceName(string entryURL) {

            for (int i = 0; i < entryURL.Length; ++i) {
                if (entryURL[i] == 'W') {
                    return entryURL.Substring(i);
                }
            }
            return null;
        }

        // it is deprecated because it could suspend in critical regions
        // creating deadlocks, since we do not share data between threads
        // this is not a problem
#pragma warning disable 0618

        public void Slow(int delay) {
            logger.Log("Slowing down for " + delay / 1000 + " seconds.");

            new Thread(() => {
                try {
                    if (workerThread != null) {
                        workerThread.Suspend();
                    }
                    Thread.Sleep(delay);
                    if (workerThread != null) {
                        workerThread.Resume();
                    }
                } catch (ThreadStateException e) {
                    logger.Log("SLOWW: " + e.Message);
                }
            }).Start();

        }

        // Function for freezing the worker
        public void FreezeW() {

            if (workerThread != null) {
                try {
                    workerThread.Suspend();
                } catch (ThreadStateException e) {
                    logger.Log("FREEZEW: " + e.Message);
                }
            }

            freezeWorkerEvent.Reset();
            freezeWorkerEvent.WaitOne();

        }

        // Function for unfreezing the worker
        public void UnfreezeW() {

            freezeWorkerEvent.Set();
            if (workerThread != null) {
                try {
                    workerThread.Resume();
                } catch (ThreadStateException e) {
                    logger.Log("UNFREEZEW: " + e.Message);
                }
            }

        }
#pragma warning restore 0618

        // Functions for freezing and unfreezing the tracker component
        // of the worker/tracker module.
        // Tracker functions include:
        // - deciding how to split a job
        // - assigning map jobs to workers
        // - warning the client the job is done
        // - detecting faulty nodes
        public void FreezeTracker() {

            // Stop the lifeproofs
            if (lifeProofTimer != null) {
                lifeProofTimer.Interval = Int32.MaxValue; // stops the timer
            }

            freezeWorkerEvent.Reset();
            freezeWorkerEvent.WaitOne();

            //service.IsCommunicationFrozen = true;
        }

        public void UnfreezeTracker() {

            if (lifeProofTimer != null) {
                lifeProofTimer.Interval = TIME_BETWEEN_LIFEPROOFS; // resets the timer
            }

            //service.FreezeTrackerEvent.Set();
        }
    }
}

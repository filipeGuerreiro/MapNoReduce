using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

using MapNoReduce;

namespace CommonLib {

    /// <summary>
    /// WorkerService class.
    /// 
    /// Exposes the worker services. Using this service is
    /// possible to introduce errors for testing, and do
    /// job syncronization with other workers in the system.
    /// </summary>
    public class WorkerService : Service {

        //private delegate void AsyncSubmitCallback();

        private delegate void AsyncSetWorkerSubmitContext(string clientURL, string mapClass, byte[] dllCode, IList<split> splits, int workerControllerId);
        private delegate void AsyncSetWorkerResumeContext(string clientURL, string mapClass, byte[] dllCode, IList<split> splits);

        private int id;
        public int Id {
            get { return id; }
        }

        private LoggerWrapper logger;

        private IWorker worker;

        // submit metadata
        private string clientURL;
        private IMapper submitedMap;
        private IList<split> splitsToFetch;
        private string mapClass;
        private byte[] code;

        // these members should likely go to the tracker class
        private IList<split> jobList;
        private WorkersManager manager;

        public IList<String> ListWorkers {
            get {
                IList<String> res = new List<String>();
                manager.ForEach((OverseenWorker o) => {
                    res.Add(o.Service.ServiceURL);
                });

                return res;
            }
        }

        private bool isCommunicationFrozen = false;
        public bool IsCommunicationFrozen {
            get { return isCommunicationFrozen; }
            set { isCommunicationFrozen = value; }
        }

        private static ManualResetEvent freezeTrackerEvent = new ManualResetEvent(false);
        public ManualResetEvent FreezeTrackerEvent {
            get { return freezeTrackerEvent; }
        }

        /// <summary>
        /// Information for keeping track of workers.
        /// </summary>
        private const Double TIME_TO_WAIT_FOR_WORKER_LIFEMSG = 10000;
        private const int TIME_TO_WAIT_FOR_WORKER_TO_COMPLETE_JOB = 10000;

        [Serializable]
        public class OverseenWorker {
            public int WorkerId;
            public WorkerService Service;
            public System.Timers.Timer LifeproofTimer;
            public JobAssignment Assignment;

            public OverseenWorker(WorkerService service, System.Timers.Timer lifeproofTimer) {
                this.WorkerId = service.Id;
                this.Service = service;
                this.LifeproofTimer = lifeproofTimer;
                this.Assignment = null;
            }

            public override bool Equals(object obj) {
                return obj is OverseenWorker && ((OverseenWorker)obj).WorkerId == WorkerId;
            }

            public override int GetHashCode() {
                return WorkerId;
            }
        }

        private class WorkersManager {
            private IList<OverseenWorker> listWorkers = new List<OverseenWorker>();
            public IList<OverseenWorker> ListWorkers {
                get { return listWorkers; }
            }

            public int Count {
                get { return listWorkers.Count; }
            }

            public bool ProbeWorker(int workerId) {
                OverseenWorker o = GetOverseenWorker(workerId);
                try {
                    o.Service.ProbeObject();
                    return true;
                } catch (Exception) {
                    return false;
                }
            }

            public void ForEach(Action<OverseenWorker> function) {
                IList<int> brokenWorkers = new List<int>();
                foreach (OverseenWorker o in listWorkers) {
                    try {
                        o.Service.ProbeObject();
                        function(o);
                    } catch (Exception) {
                        brokenWorkers.Add(o.WorkerId);
                    }
                }
                foreach (int i in brokenWorkers) {
                    RemoveOverseenWorker(i);
                }
            }

            public IList<WorkerService> GetWorkersService() {
                IList<WorkerService> list = new List<WorkerService>();
                foreach (OverseenWorker o in listWorkers) {
                    list.Add(o.Service);
                }
                return list;
            }

            public void AddOverseenWorker(OverseenWorker w) {
                OverseenWorker o = GetOverseenWorker(w.WorkerId);
                if (o != null) {
                    if (o.LifeproofTimer == null) {
                        o.LifeproofTimer = w.LifeproofTimer;
                    }
                } else {
                    listWorkers.Add(w);
                }
            }

            public OverseenWorker RemoveOverseenWorker(int workerId) {
                OverseenWorker res = GetOverseenWorker(workerId);
                listWorkers.Remove(res);
                return res;
            }

            public OverseenWorker GetOverseenWorker(int workerId) {
                try {
                    return listWorkers.First((OverseenWorker o) => (o.WorkerId == workerId));
                } catch (InvalidOperationException) {
                    return null;
                }
            }

            public bool Exists(int workerId) {
                return listWorkers.Any((OverseenWorker o) => (o.WorkerId == workerId));
            }
        }

        public WorkerService(IWorker worker) {
            this.worker = worker;
            this.id = worker.GetId();

            manager = new WorkersManager();
            manager.AddOverseenWorker(new OverseenWorker(this, null)); // Adds himself as an available worker

            logger = new LoggerWrapper("WS" + id);
            logger.Log("Created a new WorkerService");
        }


        /// <summary>
        /// This function is called by the client application to
        /// the master worker/tracker so that the size of each
        /// split can be calculated and the list of jobs be
        /// prepared to give to the other workers.
        /// </summary>
        /// <param name="fileSize"></param>
        /// <param name="nSplits"></param>
        public void CalculateJobList(int fileSize, int nSplits) {
            logger.Log("Calculating job list on fileSize: " + fileSize + " with " + nSplits + " splits.");

            checkForFrozenCommunication();

            jobList = new List<split>();

            // Calculate partition size, i.e. number of lines per split
            double partitionSize = 0;

            if (fileSize >= nSplits) {
                partitionSize = (1.0 * fileSize) / nSplits;
                partitionSize = Math.Ceiling(partitionSize);

            } else { // if number of splits is bigger than the number of lines...
                partitionSize = 1; // each worker will process just 1 line
                nSplits = fileSize; // the number of splits will not exceed the number of lines
            }

            int splitId = 0;
            for (int i = 0; i < fileSize; i += (int)partitionSize) {
                split workerJob;
                int lastLine = i + (int)partitionSize - 1;
                if (lastLine >= fileSize) { // final case when last element cant get as many lines
                    lastLine = fileSize - 1;
                }
                workerJob = new split(splitId, i, lastLine);
                jobList.Add(workerJob);
                //logger.Log("Job " + splitId + " { " + workerJob.from + ", " + workerJob.to + " }");
                splitId++;
            }
        }

        /// <summary>
        /// The assignJobs method takes the list of job
        /// tasks to be done, looks at the number of
        /// available workers, and calculates the list
        /// of tasks each worker has to do.
        /// </summary>
        /// <returns>listJobAssignments: a list of workers and their assigned splits</returns>
        private IList<JobAssignment> assignJobs() {
            IList<JobAssignment> listJobAssignments = new List<JobAssignment>();

            int nWorkers = manager.Count + 1; // Counts himself as well
            int nJobs = jobList.Count;

            int[] workerJobs = new int[nWorkers - 1];

            for (int i = 0; i < nJobs; i++) {
                workerJobs[i % (nWorkers - 1)] = 1 + workerJobs[i % (nWorkers - 1)];
            }

            int j = 0;
            for (int i = 0; i < nWorkers - 1; i++) {
                JobAssignment jobAssignment;
                List<split> jobs = new List<split>();
                //logger.Log("Worker W" + manager.ListWorkers[i].WorkerId + " will do:");

                for (int k = 0; k < workerJobs[i]; k++) {
                    jobs.Add(jobList[j]);
                    //logger.Log("--- { " + jobList[j].from + ", " + jobList[j].to + " }");
                    j++;
                }

                jobAssignment = new JobAssignment(manager.ListWorkers[i].WorkerId, jobs);
                manager.ListWorkers[i].Assignment = jobAssignment;
                listJobAssignments.Add(jobAssignment);
            }

            return listJobAssignments;
        }

        /// <summary>
        /// This method is responsible for obtaining the list
        /// of available workers and deliver to each one their
        /// piece of the job list.
        /// The clientURL is passed so that it is
        /// transferred to each one, so they know who to
        /// deliver it to when they're done.
        /// Alongside the clientURL, the name of the mapper class
        /// and the code (in bytes) containing said class is also passed,
        /// so it can be loaded by the worker at run-time.
        /// </summary>
        /// <param name="clientURL"></param>
        /// <param name="mapClass"></param>
        /// <param name="code"></param>
        public void SubmitJob(string clientURL, string mapClass, byte[] code) {

            if (code == null) {
                return;
            }

            this.mapClass = mapClass;
            this.code = code;

            logger.Log("Submit BEGIN");

            // Calculate what each worker is going to do
            IList<JobAssignment> listJobAssignments = assignJobs();

            // Call each worker to process their job
            foreach (JobAssignment jobAssignment in listJobAssignments) {

                int workerId = jobAssignment.WorkerId;

                logger.Log("Submitting job to W" + workerId);
                checkForFrozenCommunication();

                int workerControllerId = (workerId + 1) % (manager.Count + 1);

                if (workerControllerId == 0)
                    workerControllerId = 1;

                logger.Log(" Woker:  " + workerId + " Will be controlled by:  " + workerControllerId);

                if (workerId == -1 || workerId == id) { // Checks if assignment is for himself
                    AsyncSetWorkerSubmitContext submit = new AsyncSetWorkerSubmitContext(this.LoadSubmitContext);
                    submit.BeginInvoke(clientURL, mapClass, code, jobAssignment.Jobs, workerControllerId, null, null);
                } else {
                    try {
                        WorkerService worker = manager.GetOverseenWorker(workerId).Service;
                        worker.LoadSubmitContext(clientURL, mapClass, code, jobAssignment.Jobs, workerControllerId);
                    } catch (Exception) {
                        continue;
                    }
                }
            }
        }

        /// <summary>
        /// Loads metadata to process the submit command.
        /// </summary>
        /// <param name="clientURL"></param>
        /// <param name="mapClass"></param>
        /// <param name="dllCode"></param>
        public void LoadSubmitContext(string clientURL, string mapClass, byte[] dllCode, IList<split> splits, int workerControllerId) {
            if (dllCode == null) {
                return;
            }

            this.clientURL = clientURL;
            submitedMap = loadMapperObject(mapClass, dllCode);
            this.splitsToFetch = splits;

            worker.SetOperationSubmit(clientURL, submitedMap, splits, workerControllerId);
        }



        public void UpdateSubmit(int controlledId, IList<split> splits, List<string> FileObtained) {
            worker.SetControlledWorkerInfo(controlledId, splits, FileObtained);
        }

        public void UpdateOutputSplit(int controlledId, Dictionary<int, IList<KeyValuePair<string, string>>> mapResult) {
            worker.SetControlledWorkerOutput(controlledId, mapResult);
        }

        /// <summary>
        /// Obtains the Mapper object passed at runtime through
        /// the client using Reflection.
        /// </summary>
        /// <param name="mapClass"></param>
        /// <param name="code"></param>
        /// <returns></returns>
        private IMapper loadMapperObject(string mapClass, byte[] code) {
            Assembly assembly = Assembly.Load(code);

            // Walk through each type in the assembly looking for our class
            foreach (Type type in assembly.GetTypes()) {
                if (type.IsClass == true) {
                    if (type.FullName.EndsWith("." + mapClass)) {

                        // create an instance of the object
                        object ClassObj = Activator.CreateInstance(type);
                        return (IMapper)ClassObj;
                    }
                }
            }
            return null;
        }

        public void SetMasterTracker(WorkerService service) {
            worker.SetMasterTracker(service);
        }

        public WorkerService GetMasterTracker() {
            return worker.GetMasterTracker();
        }

        public void SetMasterTrackerFather(WorkerService service) {
            worker.SetMasterTrackerFather(service);
        }

        public WorkerService GetMasterTrackerFather() {
            return worker.GetMasterTrackerFather();
        }

        private System.Timers.Timer createCheckFaultyWorkerTimer(WorkerService worker) {
            System.Timers.Timer newTimer = new System.Timers.Timer();
            int workerId = worker.Id;

            newTimer.Elapsed += (sender, args) => {
                OnFaultyWorkerTimeExpiredEvent(newTimer, new FaultyWorkerEventArgs(workerId));
            };
            newTimer.Interval = TIME_TO_WAIT_FOR_WORKER_LIFEMSG;
            newTimer.Enabled = true;

            return newTimer;
        }

        /// <summary>
        /// This method is called by the workers when they are created
        /// so that the tracker can keep track them.
        /// </summary>
        /// <param name="newWorker"></param>
        public void AddWorker(WorkerService newWorker, bool isPropagate = false) {
            worker.PropagateWorker(newWorker);

            // check if it already contains this worker
            if (manager.Exists(newWorker.Id)) { return; }

            if (!isPropagate) {
                newWorker.SetMasterTracker(this);
                newWorker.SetMasterTrackerFather(GetMasterTracker());

                System.Timers.Timer newTimer = createCheckFaultyWorkerTimer(newWorker);

                // Finally, add the new worker to the list
                manager.AddOverseenWorker(new OverseenWorker(newWorker, newTimer));
            } else {
                manager.AddOverseenWorker(new OverseenWorker(newWorker, null));
            }

            logger.Log("Received new worker notification from W" + newWorker.Id);

            // Let the other workers know about this new worker
            foreach (OverseenWorker otherWorker in manager.ListWorkers) {
                if (otherWorker.WorkerId != id) {
                    WorkerService otherWorkerService = otherWorker.Service;
                    otherWorkerService.ReceiveNewWorkerUpdate(otherWorker);
                }
            }

            // Update the new worker with the list of workers in the network
            newWorker.ReceiveCurrentWorkerList(manager.ListWorkers);

        }

        /// <summary>
        /// This method is used when receiving an update from another tracker
        /// about a new worker that entered the network.
        /// WARNING: this node won't provide fault tolerance if the worker fails in the
        /// future! Must be warned by the Master tracker of that node!
        /// </summary>
        /// <param name="newWorkerURL"></param>
        public void ReceiveNewWorkerUpdate(OverseenWorker newWorker) {
            manager.AddOverseenWorker(newWorker);
        }

        /// <summary>
        /// Method used by the warned tracker on the new worker
        /// so that the new worker can know the other workers in the network.
        /// </summary>
        /// <param name="listWorkerUrls"></param>
        public void ReceiveCurrentWorkerList(IList<OverseenWorker> listWorker) {

            foreach (OverseenWorker worker in listWorker) {
                manager.AddOverseenWorker(worker);
            }
        }


        /// <summary>
        /// Method used by the worker to let this tracker know
        /// that it's alive, resetting the timer in the process.
        /// </summary>
        /// <param name="workerURL"></param>
        /// <returns>This service master tracker.</returns>
        public WorkerService ReceiveLifeproof(int workerId, WorkerService proofWorker) {

            //logger.Log("Received lifeproof from: W" + workerId);

            // if worker recovered, or re-linked from a crashed MasterTracker, reuse it.
            AddWorker(proofWorker);

            foreach (OverseenWorker worker in manager.ListWorkers) {
                if (worker.WorkerId == workerId) {
                    // Resets the timer for this worker
                    if (worker.LifeproofTimer != null) {
                        worker.LifeproofTimer.Stop();
                        try {
                            worker.LifeproofTimer.Start();
                        } catch (Exception) {
                            continue;
                        }
                    }
                }
            }

            return GetMasterTracker();
        }

        /// <summary>
        /// Lets this tracker know that the job assigned to 
        /// 'workerURL' has been done. So, remove the job from 
        /// the jobAssignment list and from the worker reference.
        /// </summary>
        /// <param name="workerURL"></param>
        public void ReceiveWorkDoneNotify(int workerId) {

            OverseenWorker worker = manager.GetOverseenWorker(workerId);

            if (worker != null) {
                worker.Assignment = null;
            }

            WorkerService masterTracker = GetMasterTracker();

            if (masterTracker != null) {
                masterTracker.ReceiveWorkDoneNotify(workerId);
            } else {
                logger.Log("WORKER DONE: W" + workerId);

                Thread checkJobStatusThread;
                checkJobStatusThread = new Thread(new ThreadStart(checkForStruggleWorkers));
                checkJobStatusThread.Start();
            }
        }

        /// <summary>
        /// Removes the worker from this services and all MasterTrackers.
        /// </summary>
        /// <param name="workerId"></param>
        public void DisposeWorker(int workerId) {
            OverseenWorker o = manager.RemoveOverseenWorker(workerId);
            if (o != null) {
                if (o.LifeproofTimer != null) {
                    o.LifeproofTimer.Dispose();
                }

                WorkerService s = GetMasterTracker();

                if (s != null) {
                    s.DisposeWorker(workerId);
                }
            }
        }

        private void checkForStruggleWorkers() {

            while (true) {

                Thread.Sleep(TIME_TO_WAIT_FOR_WORKER_TO_COMPLETE_JOB);

                foreach (OverseenWorker worker in manager.ListWorkers) {

                    // check if the other nodes are not slow
                    if (worker.Assignment != null) {
                        logger.Log("WORKER W" + worker.WorkerId + " IS A STRAGGLER");

                        foreach (OverseenWorker otherWorker in manager.ListWorkers) {

                            if (otherWorker.Assignment == null) {
                                logger.Log("Assigning work to: W" + otherWorker.WorkerId);
                                otherWorker.Assignment = worker.Assignment;
                                if (otherWorker.Assignment != null) {
                                    otherWorker.Assignment.WorkerId = otherWorker.WorkerId;

                                    int workerControllerId = (otherWorker.WorkerId + 1) % (manager.Count + 1);

                                    if (workerControllerId == 0)
                                        workerControllerId = 1;

                                    // Gets the service for the worker, but checks if that worker is himself
                                    WorkerService otherWorkerService = null;
                                    if (otherWorker.WorkerId == id) {
                                        AsyncSetWorkerSubmitContext submit = new AsyncSetWorkerSubmitContext(this.LoadSubmitContext);
                                        submit.BeginInvoke(clientURL, mapClass, code, otherWorker.Assignment.Jobs, workerControllerId, null, null);
                                    } else {
                                        otherWorkerService = otherWorker.Service;
                                        otherWorkerService.LoadSubmitContext(clientURL, this.mapClass, this.code, otherWorker.Assignment.Jobs, workerControllerId);
                                    }

                                    // No need to loop back again since someone else did the job
                                    worker.Assignment = null;
                                    return;
                                }

                            }
                        }

                    }

                }

                return;
            }

        }

        // Blocks himself if the communication is frozen
        // Returns true if it was frozen, or false otherwise
        private bool checkForFrozenCommunication() {
            if (isCommunicationFrozen == true) {

                //freezeTrackerEvent.Reset();
                freezeTrackerEvent.WaitOne();

                isCommunicationFrozen = false;
                return true;
            }
            return false;
        }

        /// <summary>
        /// Loads metadata to resume a submit command.
        /// </summary>
        /// <param name="clientURL"></param>
        /// <param name="mapClass"></param>
        /// <param name="dllCode"></param>
        public void LoadResumeContext(string clientURL, string mapClass, byte[] dllCode, IList<split> splits) {
            if (dllCode == null) {
                return;
            }

            this.clientURL = clientURL;
            submitedMap = loadMapperObject(mapClass, dllCode);
            this.splitsToFetch = splits;

            worker.ResumeOperationSubmit(clientURL, submitedMap, splits);
        }

        /// <summary>
        /// Event fired when timer expires, removing the worker 
        /// from the available worker pool.
        /// Also needs to reassign the job that the worker
        /// was in charge of so someone else can finish it.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void OnFaultyWorkerTimeExpiredEvent(System.Timers.Timer timer, FaultyWorkerEventArgs args) {
            WorkerFailureHandle(args);
        }

        public void WorkerFailureHandle(FaultyWorkerEventArgs args) {
            OverseenWorker worker = null;

            worker = manager.GetOverseenWorker(args.WorkerId);
            if (worker == null) { return; }

            DisposeWorker(worker.WorkerId);

            if (worker.WorkerId == id) { return; } // TODO: this just hides the phantom worker bug
            if (worker.Assignment == null) {
                logger.Log("WORKER FAILURE: W" + args.WorkerId);
                return;
            }

            logger.Log("WORKER FAILURE WITH JOBS TO DO: W" + args.WorkerId);

            int workerControllerId = (worker.WorkerId + 1) % (manager.Count + 1);

            if (workerControllerId == 0)
                workerControllerId = 1;

            OverseenWorker controllerWorker = manager.GetOverseenWorker(workerControllerId);

            logger.Log("JUST CHEKING THE CONTROLLER OF WORKER " + worker.WorkerId + " IS THE RIGHT ONE " + controllerWorker.WorkerId);

            if (controllerWorker != null) {

                while (true) {
                    // Reassign work - search for the first worker that is free

                    if (controllerWorker.Assignment == null) {
                        logger.Log("Assigning work to: W" + controllerWorker.WorkerId);
                        controllerWorker.Assignment = worker.Assignment;
                        if (controllerWorker.Assignment != null) {
                            controllerWorker.Assignment.WorkerId = controllerWorker.WorkerId;
                        }

                        // Gets the service for the worker, but checks if that worker is himself
                        WorkerService otherWorkerService = null;
                        if (controllerWorker.WorkerId == Id) {
                            AsyncSetWorkerResumeContext submit = new AsyncSetWorkerResumeContext(this.LoadResumeContext);
                            submit.BeginInvoke(clientURL, mapClass, code, controllerWorker.Assignment.Jobs, null, null);
                        } else {
                            otherWorkerService = controllerWorker.Service;
                            otherWorkerService.LoadResumeContext(clientURL, this.mapClass, this.code, controllerWorker.Assignment.Jobs);
                        }

                        return;
                    }
                    // What if you don't find any free worker?
                    // Sleep for 5 seconds and go to the beginning of the loop
                    Thread.Sleep(5000);
                }

            } else {
                // Do this ad infinitum until finding a free worker (in case they are all busy atm)
                while (true) {
                    // Reassign work - search for the first worker that is free
                    foreach (OverseenWorker otherWorker in manager.ListWorkers) {

                        if (otherWorker.Assignment == null) {
                            logger.Log("Assigning work to: W" + otherWorker.WorkerId);
                            otherWorker.Assignment = worker.Assignment;
                            if (otherWorker.Assignment != null) {
                                otherWorker.Assignment.WorkerId = otherWorker.WorkerId;
                            }

                            // Gets the service for the worker, but checks if that worker is himself
                            WorkerService otherWorkerService = null;
                            if (otherWorker.WorkerId == Id) {
                                AsyncSetWorkerResumeContext submit = new AsyncSetWorkerResumeContext(this.LoadResumeContext);
                                submit.BeginInvoke(clientURL, mapClass, code, otherWorker.Assignment.Jobs, null, null);
                            } else {
                                otherWorkerService = otherWorker.Service;
                                otherWorkerService.LoadResumeContext(clientURL, this.mapClass, this.code, otherWorker.Assignment.Jobs);
                            }

                            return;
                        }
                    }
                    // What if you don't find any free worker?
                    // Sleep for 5 seconds and go to the beginning of the loop
                    Thread.Sleep(5000);
                }
            }

        }

        /// <summary>
        /// Class for holding the workerURL so the timer can know
        /// who to remove when the time elapses.
        /// </summary>
        public class FaultyWorkerEventArgs : EventArgs {
            private readonly int workerId;
            public int WorkerId {
                get { return this.workerId; }
            }

            public FaultyWorkerEventArgs(int workerId) {
                this.workerId = workerId;
            }
        }

        private OverseenWorker findWorker(int workerId) {
            return manager.GetOverseenWorker(workerId);
        }

        private IList<WorkerService> getListOfWorkers(IList<OverseenWorker> listWorkers) {
            IList<WorkerService> res = new List<WorkerService>();

            foreach (OverseenWorker worker in listWorkers) {
                res.Add(worker.Service);
            }

            return res;
        }
    }
}

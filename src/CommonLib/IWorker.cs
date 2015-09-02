using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using MapNoReduce;

namespace CommonLib {

    public enum WORKER_OPERATION {
        SUBMIT, EXIT, NONE
    }

    public struct workerContext {
        // Worker locks
        private Object mainLock;
        public Object MainLock {
            get { return mainLock; }
        }

        // Worker context
        public WORKER_OPERATION Operation;
        public string ClientURL;
        public IMapper SubmitedMap;
        public IList<split> SplitsToFetch;
        public int workerControllerId;

        // in milliseconds
        public int SlowTime;

        public workerContext(Object mainLock) {
            this.mainLock = mainLock;

            Operation = WORKER_OPERATION.NONE;
            ClientURL = null;
            SubmitedMap = null;
            SplitsToFetch = null;
            workerControllerId = 0;

            SlowTime = 0;
        }
    }

    public interface IWorker {
        WorkerService GetService();

        int GetId();

        void SetMasterTracker(WorkerService service);

        WorkerService GetMasterTracker();

        void SetMasterTrackerFather(WorkerService service);

        WorkerService GetMasterTrackerFather();

        void PropagateWorker(WorkerService worker);

        void SetOperationSubmit(string clientURL, IMapper submitedMap, IList<split> splitsToFetch, int workerControllerId);

        void SetControlledWorkerInfo(int controlledId, IList<split> splits, List<string> FileObtained);

        void SetControlledWorkerOutput(int controlledId, Dictionary<int, IList<KeyValuePair<string, string>>> mapResult);

        void ResumeOperationSubmit(string clientURL, IMapper submitedMap, IList<split> splitsToFetch);

        void Slow(int slowTime);

        void FreezeW();

        void UnfreezeW();

        string GetStatus();

        void ShowStatus();

        void FreezeTracker();

        void UnfreezeTracker();

        void Die();
    }
}

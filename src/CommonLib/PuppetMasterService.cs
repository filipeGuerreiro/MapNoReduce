using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization.Formatters;

using CommonLib;
using System.Threading;

namespace PuppetMaster {

    public delegate void PuppetMasterChanged(IList<IWorker> worker, int id);

    /**
     * The PuppetMaster is responsible for:
     * - injecting failures/delays to the system;
     * - creating instances of worker processes;
     * - submitting jobs.
     * There is one PuppetMaster per machine in the system.
     **/
    public class PuppetMasterService : Service {

        private IPuppetMaster puppetMaster;

        private LoggerWrapper logger;
        private IList<IWorker> workers;
        public IList<IWorker> Workers {
            get { return workers; }
        }

        private PuppetMasterChanged onChangeCallbacks;
        public event PuppetMasterChanged OnChange {
            add { onChangeCallbacks += value; }
            remove { if (onChangeCallbacks != null) onChangeCallbacks -= value; }
        }

        public PuppetMasterService(IPuppetMaster puppetMaster) {
            this.puppetMaster = puppetMaster;
            this.logger = puppetMaster.GetLogger();
            workers = new List<IWorker>();
        }

        public IWorker GetWorker(int id) {
            return workers.First((IWorker w) => (w.GetId() == id));
        }

        public void KillWorker(int id) {
            IWorker w = GetWorker(id);
            w.Die();
            workers.Remove(w);

            if (onChangeCallbacks != null) {
                onChangeCallbacks(workers, id);
            }
        }

        /**
         * Creates a worker process with an identifier <ID> that exposes its services at <SERVICE-URL>. 
         * If an <ENTRY-URL> is provided, the new worker should notify the set of existing workers 
         * that it has started by calling the worker listening at <ENTRY-URL>.
         */
        public void CreateWorker(int id, string serviceURL, string entryURL = null) {
            IWorker newWorker = puppetMaster.CreateWorker(id, serviceURL, entryURL);

            workers.Add(newWorker);

            if (onChangeCallbacks != null) {
                onChangeCallbacks(workers, id);
            }

            logger.Log("Created worker with id: " + id + " at " + serviceURL);
        }

        // Function that ask the worker for his status
        public void ShowStatus() {
            foreach (IWorker worker in workers) {
                worker.ShowStatus();
            }
        }

        // Function that injects a delay on the worker
        public bool SlowWorker(int id, int delay) {
            foreach (IWorker w in workers) {
                if (w.GetId() == id) {
                    //w.Slow(delay);
                    logger.Log("Slowing down for " + delay / 1000 + " seconds.");
                    w.Slow(delay);
                    return true;
                }
            }
            return false;
        }

        // freeze the worker
        public bool FreezeWorker(int id) {
            foreach (IWorker worker in workers) {
                if (worker.GetId() == id) {
                    logger.Log("Freezing worker " + id);
                    worker.FreezeW();
                    return true;
                }
            }
            return false;
        }

        // Unfreeze the worker
        public bool UnfreezeWorker(int id) {
            foreach (IWorker worker in workers) {
                if (worker.GetId() == id) {
                    //worker.UnfreezeW(id);
                    logger.Log("Unfreezing worker " + id);
                    worker.UnfreezeW();
                    return true;
                }
            }
            return false;
        }

        // freeze the worker track component
        public bool FreezeTracker(int id) {
            foreach (IWorker worker in workers) {
                if (worker.GetId() == id) {
                    logger.Log("Freezing tracker " + id);

                    new Thread(() => worker.FreezeTracker()).Start();
                    
                    return true;
                }
            }
            return false;
        }

        // Unfreeze the worker track component
        public bool UnfreezeTracker(int id) {
            foreach (IWorker worker in workers) {
                if (worker.GetId() == id) {
                    logger.Log("Unfreezing tracker " + id);
                    
                    new Thread(() => worker.UnfreezeTracker()).Start();
                    
                    return true;
                }
            }
            return false;
        }
    }
}

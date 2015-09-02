using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization.Formatters;
using System.Windows.Forms;

using CommonLib;
using System.Collections;
using System.Threading;
using System.Net.Sockets;

namespace ClientLib {

    struct WriteResultContext {
        public IList<IList<KeyValuePair<string, string>>> result;
        public IList<split> splitsToFetch;
        public string outputDir;

        public WriteResultContext(IList<IList<KeyValuePair<string, string>>> result,
                IList<split> splitsToFetch, string outputDir) {
            this.result = result;
            this.splitsToFetch = splitsToFetch;
            this.outputDir = outputDir;
        }
    }

    /// <summary>
    /// Client class.
    /// 
    /// This class initiates a ClientService instance
    /// that is responsible for exposing the input file
    /// to workers, receives output and is notified
    /// when the submit is completed.
    /// </summary>
    public class Client : IClient {

        private WorkerService workerService;

        //private int clientPort;
        //private string clientName;
        private string clientURL;

        // The client (as in the worker) has one unique service to communicate with
        // others it isn't a service neither create more services when submits new jobs
        private ClientService service;
        public ClientService Service {
            get { return service; }
        }

        private Queue<WriteResultContext> toWrite = new Queue<WriteResultContext>();

        // Client thread
        public Thread writeResultThread;

        public Thread WriteResultThread {
            get { return writeResultThread; }
        }

        public Client(string workerURL) {
            //this.workerURL = workerURL;

            // Registers a one time channel to communicate with the puppet master
            //TcpChannel channel = new TcpChannel();
            //ChannelServices.RegisterChannel(channel, true);

            service = new ClientService(this);

            registerService();

            writeResultThread = new Thread(new ThreadStart(doWriteJob));
            writeResultThread.Start();
            /**
             * 
             */
        }

        /// <summary>
        /// This function is called before submit so that
        /// the client can get a reference to the master
        /// worker/tracker to which to submit jobs.
        /// </summary>
        /// <param name="entryURL"></param>
        /// <param name="entryServiceName"></param>
        public void SetupWorkerChannel(string entryURL, string entryServiceName) {
            workerService =
                (WorkerService)Factory.Instance.GetService(
                    typeof(WorkerService), entryURL);

            // catch this on console
            if (workerService == null) {
                throw new RemotingException("error: cannot locate worker!");
            }

            workerService.AddWorker(workerService);
        }

        /// <summary>
        /// Submits a job to the MapNoReduce system.
        ///
        /// This function will send the file size and
        /// number of splits to the master tracker.
        /// After that, the client will send it's URL
        /// so that he can wait for all the responses
        /// from the system before returning.
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="nSplits"></param>
        /// <param name="outputDir"></param>
        /// <param name="mapFunction"></param>
        /// <param name="entryURL"></param>
        public void Submit(string entryURL, string filePath, string outputDir,
                int nSplits, string mapFunction, string dllPath) {

            string serviceName = parseServiceName(entryURL);
            SetupWorkerChannel(entryURL, serviceName);

            this.service.OutputDir = outputDir;

            IList<String> file = parseFile(filePath);
            service.InputFile = file;

            int fileSize = file.Count;
            workerService.CalculateJobList(fileSize, nSplits);

            byte[] code = File.ReadAllBytes(dllPath);
            workerService.SubmitJob(clientURL, mapFunction, code);
            // ...

        }


        // Notify the user that the job is done
        // TODO: show a messagebox 
        public void NotifyUser() { }

        public void AsyncWriteResult(IList<IList<KeyValuePair<string, string>>> result,
                IList<split> splitsToFetch, string outputDir) {
            lock (toWrite) {
                toWrite.Enqueue(new WriteResultContext(result, splitsToFetch, outputDir));
                Monitor.PulseAll(toWrite);
            }
        }

        // ----------------------- Implementation methods -------------------------

        private static IList<String> parseFile(String filePath) {
            List<string> fileLines = new List<string>(File.ReadLines(filePath));
            return fileLines;
        }

        /// <summary>
        /// This function exposes the client's service
        /// to the workers, so they can ask for files and
        /// submit their work when they're done.
        /// </summary>
        private void registerService(int port = Constants.CLIENT_PORT,
                string name = Constants.CLIENT_SERVICE_NAME) {
            clientURL = Constants.LOCALHOST_URL + ":" + port + "/" + name;
            if (!Factory.Instance.CreateService(port, name, service, typeof(ClientService), clientURL)) {
                registerService(port + 1, name);
            }
        }

        /// <summary>
        /// Parses a given entryURL for a worker, and obtains its serviceName.
        /// Note: At the moment, it assumes all workers prefix their serviceName
        /// with a W. It will fail otherwise.
        /// </summary>
        /// <param name="entryURL"></param>
        private string parseServiceName(string entryURL) {

            for (int i = 0; i < entryURL.Length; ++i) {
                if (entryURL[i] == 'W') {
                    return entryURL.Substring(i);
                }
            }
            return null;
        }

        private void doWriteJob() {
            do {
                WriteResultContext resultJob;
                lock (toWrite) {
                    if (toWrite.Count == 0) {
                        Monitor.Wait(toWrite);
                    }
                    resultJob = toWrite.Dequeue();
                }
                int j = 0;
                foreach (split job in resultJob.splitsToFetch) {
                    IList<KeyValuePair<string, string>> temp = resultJob.result[j];

                    String element = "Split[" + job.id + "]:\r\n";
                    foreach (KeyValuePair<string, string> el in temp) {
                        element += el.Key + ", " + el.Value + "\r\n";
                    }
                    Directory.CreateDirectory(resultJob.outputDir);
                    File.AppendAllText(resultJob.outputDir + job.id + ".out", element);
                    j++;
                }
            } while (true);
        }
    }
}

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
using System.Windows.Forms;
using System.Threading;
using System.Reflection;

using ClientLib;
using WorkerLib;
using CommonLib;
using System.Net.Sockets;

namespace PuppetMaster {
    public class PuppetMasterConsole {

        private class Manager {
            public IDictionary<String, PuppetMasterService> puppetMastersList
                = new Dictionary<String, PuppetMasterService>();

            public void ForEach(Action<PuppetMasterService> function) {
                ForEach((PuppetMasterService s) => {
                    function(s);
                    return false;
                });
            }

            public void ForEach(Func<PuppetMasterService, bool> function) {
                IList<String> brokenServices = new List<String>();
                foreach (var p in puppetMastersList) {
                    try {
                        p.Value.ProbeObject();
                        if (function(p.Value)) {
                            break;
                        }
                    } catch (Exception) {
                        brokenServices.Add(p.Key);
                    }
                }

                foreach (String k in brokenServices) {
                    puppetMastersList.Remove(k);
                }
            }

            public void AddPuppetMaster(PuppetMasterService service) {
                String key = "";
                try {
                    service.ProbeObject();
                    key = service.ServiceURL;
                } catch (Exception) {
                    return;
                }
                IList<String> brokenServices = new List<String>();
                if (!puppetMastersList.Any((KeyValuePair<String, PuppetMasterService> s) => {
                    try {
                        s.Value.ProbeObject();
                        return s.Value.ServiceURL == service.ServiceURL;
                    } catch (Exception) {
                        brokenServices.Add(s.Key);
                    }
                    return false;
                })) {
                    puppetMastersList.Add(key, service);
                }

                foreach (String k in brokenServices) {
                    puppetMastersList.Remove(k);
                }
            }
        }

        private Manager manager = new Manager();

        private String puppetMasterURL;
        public String PuppetMasterURL {
            get { return puppetMasterURL; }
        }

        Client client;

        public bool IsFormActive = false;
        private PuppetMasterForm form;
        private Thread formThread;

        private PuppetMaster puppetMaster;

        private CommandParser parser;

        private PuppetMasterService service;
        public PuppetMasterService Service {
            get { return service; }
        }

        public void SaveScript(string script) {
            Console.WriteLine();
            Console.WriteLine("Loading script: ");
            Console.WriteLine(script);
            Logger.prompt();
            parser.Script = script;
        }

        public void RunScript() {
            parser.ParseScript();
        }

        public void StepScript() {
            parser.ParseLine();
        }

        private void formThreadJob() {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            form = new PuppetMasterForm(this);
            Application.Run(form);
        }

        private void initForm() {
            formThread = new Thread(new ThreadStart(formThreadJob));
            formThread.SetApartmentState(ApartmentState.STA);
            formThread.Start();
        }

        /// <summary>
        /// Runs the console main loop.
        /// 
        /// Only the thread running this should print/read at the console.
        /// </summary>
        private void consoleJob() {
            do {
                Logger.prompt();
                string command = Console.ReadLine();
                parser.ParseLine(command);
            } while (true);
        }

        private void main(string[] args) {
            foreach (string arg in args) {
                // if it is a user-application puppetmaster
                if (arg.Contains("--Dgui")) {
                    initForm();
                }
            }
            client = new Client("");
            initConsole();
        }

        private void initConsole() {
            string serviceName = Constants.PUPPETMASTER_SERVICE_NAME;
            int port = Constants.PUPPETMASTER_PORT_START;

            puppetMaster = new PuppetMaster(serviceName);

            service = new PuppetMasterService(puppetMaster);

            initService(port, serviceName);

            Console.WriteLine("Running service at " + puppetMasterURL);
            Console.WriteLine("Accepting commands here");

            initParser();

            consoleJob();
        }

        private void initService(int port, string serviceName) {
            puppetMasterURL = Constants.LOCALHOST_URL + ":" + port + "/" + serviceName;
            if (!Factory.Instance.CreateService(port, serviceName,
                    service, typeof(PuppetMasterService), puppetMasterURL)) {
                initService(port + 1, serviceName);
            }
        }

        private void initParser() {
            parser = new CommandParser(new ProcessResult(parseCommandNotFound),
                new ProcessResult(parseEndOfScript));

            parser.AddCommand(new Command("WORKER", new ProcessResult(parseWorker),
                "id", "puppetmaster-url", "service-url", "entry-url"));
            parser.AddCommand(new Command("SUBMIT", new ProcessResult(parseSubmit),
                "entry-url", "file", "output", "s", "map", "dll"));
            parser.AddCommand(new Command("WAIT", new ProcessResult(parseWait), "delay"));
            parser.AddCommand(new Command("STATUS", new ProcessResult(parseStatus)));
            parser.AddCommand(new Command("SLOWW", new ProcessResult(parseSloww), "id", "delay"));
            parser.AddCommand(new Command("FREEZEW", new ProcessResult(parseFreezew), "id"));
            parser.AddCommand(new Command("UNFREEZEW", new ProcessResult(parseUnfreezew), "id"));
            parser.AddCommand(new Command("FREEZEC", new ProcessResult(parseFreezec), "id"));
            parser.AddCommand(new Command("UNFREEZEC", new ProcessResult(parseUnfreezec), "id"));
            parser.AddCommand(new Command("HELP", new ProcessResult(parseHelp)));

            parser.AddCommand(new Command("KILL", new ProcessResult(parseKill), "id"));
            parser.AddCommand(new Command("STARTGUI", new ProcessResult(parseStartgui)));
            parser.AddCommand(new Command("EXIT", new ProcessResult(parseExit), "option"));
        }

        private void AddPuppetMaster(PuppetMasterService pm) {
            if (pm == null) {
                return;
            }
            manager.AddPuppetMaster(pm);
        }

        private void parseWorker(IDictionary<String, String> parameters, int lineNumber) {
            string id = parameters["id"];
            string puppetMasterURL = parameters["puppetmaster-url"];
            string serviceURL = parameters["service-url"];
            string entryURL = parameters["entry-url"];

            if (!id.Equals("")
                    && !puppetMasterURL.Equals("")
                    && !serviceURL.Equals("")) {

                if (RegexConstants.PATTERN_ID.IsMatch(id)
                        && RegexConstants.PATTERN_URL.IsMatch(puppetMasterURL)) {
                    int parsedID = 0;
                    try {
                        parsedID = Int32.Parse(id);
                    } catch (FormatException) {
                        showMessage(Constants.COMMAND_WORKER_INVALID_PARAMETERS);
                    }

                    PuppetMasterService pm =
                        (PuppetMasterService)Factory.Instance.GetService(
                            typeof(PuppetMasterService), puppetMasterURL);

                    if (pm == null) {
                        showMessage("Cannot locate: " + puppetMasterURL);
                    } else {
                        AddPuppetMaster(pm);
                        createWorkerThread(pm, parsedID, serviceURL, entryURL);
                    }

                } else {
                    showMessage(Constants.COMMAND_WORKER_INVALID_PARAMETERS);
                    return;
                }
            } else {
                showMessage(Constants.COMMAND_WORKER_INVALID_SYNTAX);
            }
        }

        private void createWorkerThread(PuppetMasterService pm, int parsedID, string serviceURL, string entryURL) {
            try {
                if (entryURL.Equals("")) {
                    pm.CreateWorker(parsedID, serviceURL, null);
                } else if (RegexConstants.PATTERN_URL.IsMatch(entryURL)) {
                    pm.CreateWorker(parsedID, serviceURL, entryURL);
                } else {
                    showMessage(Constants.COMMAND_WORKER_INVALID_ENTRY_URL);
                }
            } catch (SocketException) {
                showMessage("Service not found. Maybe the PuppetMaster is down.");
            }
        }

        private void parseSubmit(IDictionary<String, String> parameters, int lineNumber) {
            string entryURL = parameters["entry-url"];
            string file = parameters["file"];
            string output = parameters["output"];
            string s = parameters["s"];
            string map = parameters["map"];
            string dllPath = parameters["dll"];

            if (!entryURL.Equals("")
                    && !file.Equals("")
                    && !output.Equals("")
                    && !s.Equals("")
                    && !map.Equals("")
                    && !dllPath.Equals("")) {
                if (RegexConstants.PATTERN_URL.IsMatch(entryURL)
                        && RegexConstants.PATTERN_FILE.IsMatch(file)
                        && RegexConstants.PATTERN_ID.IsMatch(s)) {
                    int nSplits = 0;
                    try {
                        nSplits = Int32.Parse(s);
                    } catch (Exception) {
                        showMessage(Constants.COMMAND_SUBMIT_INVALID_PARAMETERS);
                    }

                    try {
                        client.Submit(entryURL, file, output, nSplits, map, dllPath);
                    } catch (RemotingException e) {
                        showMessage(e.Message);
                    }
                } else {
                    showMessage(Constants.COMMAND_SUBMIT_INVALID_PARAMETERS);
                }
            } else {
                showMessage(Constants.COMMAND_SUBMIT_INVALID_SYNTAX);
            }
        }

        private void parseWait(IDictionary<String, String> parameters, int lineNumber) {
            string delay = parameters["delay"];
            int nSeconds;
            try {
                nSeconds = Int32.Parse(delay);
            } catch (Exception) {
                showMessage(String.Format(Constants.COMMAND_WAIT_INVALID_DELAY, delay));
                nSeconds = 0;
            }
            Thread.Sleep(nSeconds * 1000);
        }

        private void parseStatus(IDictionary<String, String> parameters, int lineNumber) {
            manager.ForEach((PuppetMasterService pm) => {
                pm.ShowStatus();
            });
        }

        private void parseSloww(IDictionary<String, String> parameters, int lineNumber) {
            string idString = parameters["id"];
            string delay = parameters["delay"];
            int id;
            int nSeconds;

            try {
                id = Int32.Parse(idString);
            } catch (Exception) {
                showMessage(String.Format(Constants.COMMAND_SLOWW_INVALID_ID, idString));
                id = 1;
                return;
            }

            try {
                nSeconds = Int32.Parse(delay);
            } catch (Exception) {
                showMessage(String.Format(Constants.COMMAND_SLOWW_INVALID_DELAY, delay));
                nSeconds = 0;
            }
            nSeconds *= 1000;

            //new Thread(() => slowWorkerThread(id, nSeconds)).Start();
            slowWorkerThread(id, nSeconds);

        }

        private void slowWorkerThread(int id, int nSeconds) {
            manager.ForEach((PuppetMasterService pm) => {
                return pm.SlowWorker(id, nSeconds); // if the pm has a worker with that id, dont ask the others
            });
        }

        private void parseFreezew(IDictionary<String, String> parameters, int lineNumber) {
            string idString = parameters["id"];
            int workerId;

            try {
                workerId = Int32.Parse(idString);
            } catch (Exception) {
                showMessage(String.Format(Constants.COMMAND_FREEZEW_INVALID_ID, idString));
                workerId = 1;
                return;
            }

            new Thread(() => freezeWorkerThread(workerId)).Start();
        }

        private void freezeWorkerThread(int workerId) {
            manager.ForEach((PuppetMasterService pm) => {
                return pm.FreezeWorker(workerId); // if the pm has a worker with that id, dont ask the others
            });
        }

        private void parseUnfreezew(IDictionary<String, String> parameters, int lineNumber) {
            string idString = parameters["id"];
            int workerId;

            try {
                workerId = Int32.Parse(idString);
            } catch (Exception) {
                showMessage(String.Format(Constants.COMMAND_UNFREEZEW_INVALID_ID, idString));
                workerId = 1;
                return;
            }

            new Thread(() => unfreezeWorkerThread(workerId)).Start();
        }

        private void unfreezeWorkerThread(int workerId) {
            manager.ForEach((PuppetMasterService pm) => {
                return pm.UnfreezeWorker(workerId); // if the pm has a worker with that id, dont ask the others
            });
        }

        private void parseFreezec(IDictionary<String, String> parameters, int lineNumber) {
            string idString = parameters["id"];
            int workerId;

            try {
                workerId = Int32.Parse(idString);
            } catch (Exception) {
                showMessage(String.Format(Constants.COMMAND_FREEZEC_INVALID_ID, idString));
                workerId = 1;
                return;
            }

            new Thread(() => freezeTrackerThread(workerId)).Start();
        }

        private void freezeTrackerThread(int workerId) {
            manager.ForEach((PuppetMasterService pm) => {
                return pm.FreezeTracker(workerId); // if the pm has a worker with that id, dont ask the others
            });
        }

        private void parseUnfreezec(IDictionary<String, String> parameters, int lineNumber) {
            string idString = parameters["id"];
            int workerId;

            try {
                workerId = Int32.Parse(idString);
            } catch (Exception) {
                showMessage(String.Format(Constants.COMMAND_UNFREEZEC_INVALID_ID, idString));
                workerId = 1;
                return;
            }

            new Thread(() => unfreezeTrackerThread(workerId)).Start();
        }

        private void unfreezeTrackerThread(int workerId) {
            manager.ForEach((PuppetMasterService pm) => {
                return pm.UnfreezeTracker(workerId); // if the pm has a worker with that id, dont ask the others
            });
        }

        private void parseHelp(IDictionary<String, String> parameters, int lineNumber) {
            showMessage(Constants.COMMAND_HELP);
        }

        private void parseKill(IDictionary<String, String> parameters, int lineNumber) {
            string idString = parameters["id"];
            int workerId;

            try {
                workerId = Int32.Parse(idString);
            } catch (Exception) {
                showMessage(String.Format(Constants.COMMAND_KILL_INVALID_ID, idString));
                workerId = 1;
                return;
            }

            service.KillWorker(workerId);
        }

        private void parseStartgui(IDictionary<String, String> parameters, int lineNumber) {
            if (!IsFormActive) {
                initForm();
            }
        }

        private void parseExit(IDictionary<String, String> parameters, int lineNumber) {
            string option = parameters["option"];
            if (option.ToUpperInvariant().Equals("GUI")) {
                Application.Exit();
            } else {
                Environment.Exit(0);
            }
        }

        private void parseCommandNotFound(IDictionary<String, String> parameters, int lineNumber) {
            showMessage(Constants.COMMAND_NOT_FOUND);
        }

        private void parseEndOfScript(IDictionary<String, String> parameters, int lineNumber) {
            showMessage(Constants.COMMAND_END_OF_SCRIPT);
        }

        private void showMessage(string msg) {
            Console.WriteLine();
            Console.WriteLine(msg);
            if (IsFormActive) {
                MessageBox.Show(msg);
            }
        }

        /// <summary>
        /// The PuppetMaster must start manually
        /// and listen on a port to receive the
        /// command to create a worker.
        /// </summary>
        /// <param name="args"></param>
        public static void Main(string[] args) {
            PuppetMasterConsole p = new PuppetMasterConsole();
            p.main(args);
        }
    }
}
